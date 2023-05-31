from os import environ
environ["PRODUCTION"] = environ["PRODUCTION"] if "PRODUCTION" in environ and environ["PRODUCTION"] else ""

from signal import signal, SIGINT, SIGTERM
from time import time
from random import randint
from datetime import datetime, timedelta
from aiohttp import TCPConnector, ClientSession
from asyncio import sleep, wait, run, gather, create_task
from uuid import uuid4
from pytz import utc
from traceback import format_exc

from discord import Webhook, Embed, File
from discord.errors import NotFound
from discord.utils import MISSING
from google.cloud.firestore import AsyncClient as FirestoreClient, DELETE_FIELD
from google.cloud.error_reporting import Client as ErrorReportingClient
from pycoingecko import CoinGeckoAPI

from helpers import constants
from assets import static_storage
from Processor import process_chart_arguments, process_heatmap_arguments, process_quote_arguments, process_task
from DatabaseConnector import DatabaseConnector
from CommandRequest import CommandRequest
from helpers.utils import seconds_until_cycle, get_accepted_timeframes


database = FirestoreClient()

NAMES = {
	"401328409499664394": ("Alpha", "https://storage.alpha.bot/Icon.png"),
	"487714342301859854": ("Alpha (Beta)", MISSING)
}


class Scheduler(object):
	accountProperties = DatabaseConnector(mode="account")
	guildProperties = DatabaseConnector(mode="guild")


	# -------------------------
	# Startup
	# -------------------------

	def __init__(self):
		self.isServiceAvailable = True
		signal(SIGINT, self.exit_gracefully)
		signal(SIGTERM, self.exit_gracefully)

		self.logging = ErrorReportingClient(service="scheduler")

	def exit_gracefully(self, signum, frame):
		print("[Startup]: Scheduler is exiting")
		self.isServiceAvailable = False


	# -------------------------
	# Job queue
	# -------------------------

	async def run(self):
		while self.isServiceAvailable:
			try:
				await sleep(seconds_until_cycle())
				t = datetime.now().astimezone(utc)
				timeframes = get_accepted_timeframes(t)

				if "1m" in timeframes:
					await self.process_posts()

			except (KeyboardInterrupt, SystemExit): return
			except Exception:
				print(format_exc())
				if environ["PRODUCTION"]: self.logging.report_exception()


	# -------------------------
	# Scheduled Posts
	# -------------------------

	async def process_posts(self):
		startTimestamp = time()
		conn = TCPConnector(limit=5)
		async with ClientSession(connector=conn) as session:
			try:
				requestMap = {}
				requests = []
				guilds = database.document("details/scheduledPosts").collections()

				async for guild in guilds:
					guildId = guild.id
					if not environ["PRODUCTION"] and guildId != "926518026457739304": continue
					print(f"Processing guild {guildId}")

					async for post in guild.stream():
						data = post.to_dict()

						if data.get("timestamp", time()) < time() - 86400 * 5:
							print(f"Deleting stale post {post.id}")
							await post.reference.delete()
							continue

						if data["start"] > time() or int(data["start"] / 60) % data["period"] != int(time() / 60) % data["period"]: continue

						if data.get("exclude") == "outside us market hours":
							today = datetime.now().astimezone(utc)
							if today.hour < 14 or (today.hour == 14 and today.minute < 30) or today.hour > 21: continue
							yesterday = today.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
							startTime = yesterday.strftime("%Y%m%d")
							url = f"https://cloud.iexapis.com/stable/ref-data/us/dates/trade/next/1/{startTime}?token={environ['IEXC_KEY']}"
							async with session.get(url) as resp:
								if resp.status != 200: continue
								resp = await resp.json()
								if resp[0]["date"] != today.strftime("%Y-%m-%d"):
									print(f"Skipping post {post.id}")
									continue
						elif data.get("exclude") == "weekends":
							weekday = datetime.now().astimezone(utc).weekday()
							if weekday == 5 or weekday == 6:
								print(f"Skipping post {post.id}")
								continue

						print(f"Processing post {post.id}")

						guild = await self.guildProperties.get(guildId, {})
						accountId = guild.get("settings", {}).get("setup", {}).get("connection")
						user = await self.accountProperties.get(accountId, {})

						if not guild:
							print(f"Deleting post {post.id} due to missing guild")
							await post.reference.delete()
						if guild.get("stale", {}).get("count", 0) > 0:
							print(f"Skipping post {post.id} due to stale guild")
							continue

						request = CommandRequest(
							accountId=accountId,
							authorId=data["authorId"],
							channelId=data["channelId"],
							guildId=guildId,
							accountProperties=user,
							guildProperties=guild
						)

						if not request.scheduled_posting_available():
							print(f"Skipping post {post.id} due to missing subscription")
							continue

						subscriptions = sorted(user["customer"]["subscriptions"].keys())
						key = f"{data['authorId']} {subscriptions} {' '.join(data['arguments'])}"
						if key in requestMap:
							print(f"Using cached response for post {post.id}")
							requestMap[key][1].append(len(requests))
						else:
							print(f"Creating new response for post {post.id}")
							requestMap[key] = [
								create_task(self.process_request(request, data)),
								[len(requests)]
							]
						requests.append((data, request, post))

				tasks = []
				for key, [response, indices] in requestMap.items():
					files, embeds = await response
					for i in indices:
						data, request, post = requests[i]
						print(f"Pushing post {post.id}")
						tasks.append(create_task(self.push_post(session, files, embeds, data, post.reference, request)))
				if len(tasks) > 0: await wait(tasks)

				print("Task finished in", time() - startTimestamp, "seconds")
			except (KeyboardInterrupt, SystemExit): pass
			except Exception:
				print(format_exc())
				if environ["PRODUCTION"]: self.logging.report_exception()

	async def process_request(self, request, data):
		try:
			botId = data.get("botId", "401328409499664394")
			origin = "default" if botId == "401328409499664394" else botId

			if data["command"] == "chart":
				platforms = request.get_platform_order_for("c")
				responseMessage, task = await process_chart_arguments(data["arguments"][1:], platforms, tickerId=data["arguments"][0].upper(), defaults=request.guildProperties["charting"])

				if responseMessage is not None:
					description = "[Advanced Charting add-on](https://www.alpha.bot/pro/advanced-charting) unlocks additional assets, indicators, timeframes and more." if responseMessage.endswith("add-on.") else "Detailed guide with examples is available on [our website](https://www.alpha.bot/features/charting)."
					embed = Embed(title=responseMessage, description=description, color=constants.colors["gray"])
					embed.set_author(name="Invalid argument", icon_url=static_storage.error_icon)
					return [], [embed]

				currentTask = task.get(task.get("currentPlatform"))
				timeframes = task.pop("timeframes")
				for p, t in timeframes.items(): task[p]["currentTimeframe"] = t[0]

				payload, responseMessage = await process_task(task, "chart", origin=origin)

				files, embeds = [], []
				if responseMessage == "requires pro":
					embed = Embed(title=f"The requested chart for `{currentTask.get('ticker').get('name')}` is only available on TradingView Premium.", description="All TradingView Premium charts are bundled with the [Advanced Charting add-on](https://www.alpha.bot/pro/advanced-charting).", color=constants.colors["gray"])
					embed.set_author(name="TradingView Premium", icon_url=static_storage.error_icon)
					embeds.append(embed)
				elif payload is None:
					errorMessage = f"Requested chart for `{currentTask.get('ticker').get('name')}` is not available." if responseMessage is None else responseMessage
					embed = Embed(title=errorMessage, color=constants.colors["gray"])
					embed.set_author(name="Chart not available", icon_url=static_storage.error_icon)
					embeds.append(embed)
				else:
					task["currentPlatform"] = payload.get("platform")
					currentTask = task.get(task.get("currentPlatform"))
					files.append(File(payload.get("data"), filename="{:.0f}-{}-{}.png".format(time() * 1000, request.authorId, randint(1000, 9999))))

				return files, embeds

			elif data["command"] == "heatmap":
				platforms = request.get_platform_order_for("hmap", assetType=data["arguments"][0])
				responseMessage, task = await process_heatmap_arguments(data["arguments"], platforms)

				if responseMessage is not None:
					embed = Embed(title=responseMessage, description="Detailed guide with examples is available on [our website](https://www.alpha.bot/features/heatmaps).", color=constants.colors["gray"])
					embed.set_author(name="Invalid argument", icon_url=static_storage.error_icon)
					return [], [embed]

				currentTask = task.get(task.get("currentPlatform"))
				timeframes = task.pop("timeframes")
				for p, t in timeframes.items(): task[p]["currentTimeframe"] = t[0]

				payload, responseMessage = await process_task(task, "heatmap", origin=origin)

				files, embeds = [], []
				if payload is None:
					errorMessage = "Requested heatmap is not available." if responseMessage is None else responseMessage
					embed = Embed(title=errorMessage, color=constants.colors["gray"])
					embed.set_author(name="Heatmap not available", icon_url=static_storage.error_icon)
					embeds.append(embed)
				else:
					files.append(File(payload.get("data"), filename="{:.0f}-{}-{}.png".format(time() * 1000, request.authorId, randint(1000, 9999))))

				return files, embeds

			elif data["command"] == "price":
				platforms = request.get_platform_order_for("p")
				responseMessage, task = await process_quote_arguments(data["arguments"][1:], platforms, tickerId=data["arguments"][0].upper())

				if responseMessage is not None:
					embed = Embed(title=responseMessage, description="Detailed guide with examples is available on [our website](https://www.alpha.bot/features/prices).", color=constants.colors["gray"])
					embed.set_author(name="Invalid argument", icon_url=static_storage.error_icon)
					return [], [embed]

				currentTask = task.get(task.get("currentPlatform"))
				payload, responseMessage = await process_task(task, "quote")

				if payload is None or "quotePrice" not in payload:
					errorMessage = f"Requested quote for `{currentTask.get('ticker').get('name')}` is not available." if responseMessage is None else responseMessage
					embed = Embed(title=errorMessage, color=constants.colors["gray"])
					embed.set_author(name="Data not available", icon_url=static_storage.error_icon)
				else:
					currentTask = task.get(payload.get("platform"))
					if payload.get("platform") in ["Alternative.me", "CNN Business"]:
						embed = Embed(title=f"{payload['quotePrice']} *({payload['change']})*", description=payload.get("quoteConvertedPrice"), color=constants.colors[payload["messageColor"]])
						embed.set_author(name=payload["title"], icon_url=payload.get("thumbnailUrl"))
						embed.set_footer(text=payload["sourceText"])
					else:
						embed = Embed(title="{}{}".format(payload["quotePrice"], f" *({payload['change']})*" if "change" in payload else ""), description=payload.get("quoteConvertedPrice"), color=constants.colors[payload["messageColor"]])
						embed.set_author(name=payload["title"], icon_url=payload.get("thumbnailUrl"))
						embed.set_footer(text=payload["sourceText"])

			elif data["command"] == "volume":
				platforms = request.get_platform_order_for("v")
				responseMessage, task = await process_quote_arguments(data["arguments"][1:], platforms, tickerId=data["arguments"][0].upper())

				if responseMessage is not None:
					embed = Embed(title=responseMessage, description="Detailed guide with examples is available on [our website](https://www.alpha.bot/features/volume).", color=constants.colors["gray"])
					embed.set_author(name="Invalid argument", icon_url=static_storage.error_icon)
					return [], [embed]

				currentTask = task.get(task.get("currentPlatform"))
				payload, responseMessage = await process_task(task, "quote")

				if payload is None or "quoteVolume" not in payload:
					errorMessage = f"Requested volume for `{currentTask.get('ticker').get('name')}` is not available." if responseMessage is None else responseMessage
					embed = Embed(title=errorMessage, color=constants.colors["gray"])
					embed.set_author(name="Data not available", icon_url=static_storage.error_icon)
				else:
					currentTask = task.get(payload.get("platform"))
					embed = Embed(title=payload["quoteVolume"], description=payload.get("quoteConvertedVolume"), color=constants.colors["orange"])
					embed.set_author(name=payload["title"], icon_url=payload.get("thumbnailUrl"))
					embed.set_footer(text=payload["sourceText"])

				return [], [embed]

			elif data["command"] == "lookup top-performers":
				[category, limit] = data["arguments"]
				if category == "crypto gainers":
					rawData = []
					cg = CoinGeckoAPI(api_key=environ["COINGECKO_API_KEY"])
					page = 1
					while True:
						rawData += cg.get_coins_markets(vs_currency="usd", order="market_cap_desc", per_page=250, page=page, price_change_percentage="24h")
						page += 1
						if page > 4: break

					response = []
					for e in rawData[:max(10, int(limit))]:
						if e.get("price_change_percentage_24h_in_currency", None) is not None:
							response.append({"symbol": e["symbol"].upper(), "change": e["price_change_percentage_24h_in_currency"]})
					response = sorted(response, key=lambda k: k["change"], reverse=True)[:10]

					embed = Embed(title="Top gainers", color=constants.colors["deep purple"])
					for token in response:
						embed.add_field(name=token["symbol"], value="Gained {:,.2f} %".format(token["change"]), inline=True)

				elif category == "crypto losers":
					rawData = []
					cg = CoinGeckoAPI(api_key=environ["COINGECKO_API_KEY"])
					page = 1
					while True:
						rawData += cg.get_coins_markets(vs_currency="usd", order="market_cap_desc", per_page=250, page=page, price_change_percentage="24h")
						page += 1
						if page > 4: break

					response = []
					for e in rawData[:max(10, int(limit))]:
						if e.get("price_change_percentage_24h_in_currency", None) is not None:
							response.append({"symbol": e["symbol"].upper(), "change": e["price_change_percentage_24h_in_currency"]})
					response = sorted(response, key=lambda k: k["change"])[:10]

					embed = Embed(title="Top losers", color=constants.colors["deep purple"])
					for token in response:
						embed.add_field(name=token["symbol"], value="Lost {:,.2f} %".format(token["change"]), inline=True)

				return [], [embed]

		except (KeyboardInterrupt, SystemExit): pass
		except Exception:
			print(data["authorId"], data["channelId"])
			print(format_exc())
			if environ["PRODUCTION"]: self.logging.report_exception()
		return [], []

	async def push_post(self, session, files, embeds, data, reference, request):
		try:
			if len(files) == 0 and len(embeds) == 0:
				return

			content = None
			if data.get("message") is not None:
				embeds.append(Embed(description=data.get("message"), color=constants.colors["purple"]))
			if data.get("tag") is not None:
				content = f"<@&{message.get('tag')}>"

			name, avatar = NAMES.get(data.get("botId", "401328409499664394"), (MISSING, MISSING))

			await Webhook.from_url(data["url"], session=session).send(
				content=content,
				files=files,
				embeds=embeds,
				username=name,
				avatar_url=avatar
			)

			if data.get("status") == "failed":
				await reference.set({"status": DELETE_FIELD, "timestamp": DELETE_FIELD}, merge=True)
		except (KeyboardInterrupt, SystemExit): pass
		except NotFound:
			print(f"Webhook not found in {request.guildId}")
			if data.get("status") != "failed":
				await database.document(f"discord/properties/messages/{str(uuid4())}").set({
					"title": "Scheduled post is failing!",
					"description": f"You have scheduled a post (`/{data['command']} {' '.join(data['arguments'])}`) to be sent to a channel that no longer exists or no longer has Alpha.bot's webhook. Use `/schedule list` to review, delete and reschedule the post if you want to keep it. If the post keeps failing, it will be automatically deleted in 5 days.",
					"subtitle": "Scheduled posts",
					"color": 6765239,
					"user": data['authorId'],
					"channel": data['channelId'],
					"backupUser": data['authorId'],
					"backupChannel": data['channelId'],
					"botId": data.get("botId", "401328409499664394")
				})
				await reference.set({"status": "failed", "timestamp": time()}, merge=True)
		except Exception:
			print(f"{request.guildId}/{data['channelId']} set by {data['authorId']}")
			print(format_exc())
			if environ["PRODUCTION"]: self.logging.report_exception()

if __name__ == "__main__":
	scheduler = Scheduler()
	run(scheduler.run())
