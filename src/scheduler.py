from os import environ
environ["PRODUCTION"] = environ["PRODUCTION"] if "PRODUCTION" in environ and environ["PRODUCTION"] else ""

from signal import signal, SIGINT, SIGTERM
from time import time
from random import randint
from io import BytesIO
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from aiohttp import TCPConnector, ClientSession
from asyncio import sleep, wait, run, gather, create_task
from uuid import uuid4
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

ALPHABOT_ID = "401328409499664394"
ALPHABOT_BETA_ID = "487714342301859854"
BOT_CONFIG = {
	ALPHABOT_ID: ("Alpha", "https://storage.alpha.bot/Icon.png", "DISCORD_PRODUCTION_TOKEN"),
	ALPHABOT_BETA_ID: ("Alpha (Beta)", MISSING, "DISCORD_PRODUCTION_TOKEN"),
	1051325737631756339: (MISSING, MISSING, "TOKEN_HG60DTuv5GYWDUVX1ENONGdvEJ43"),
	700764913257283625: (MISSING, MISSING, "TOKEN_N8V1MEBUJFSVP4IQMUXYYIEDFYI1"),
	1115262078853333052: (MISSING, MISSING, "TOKEN_H59TRYWEQLSI0U1UZLDFZRTXPXC2"),
	1119204848634703885: (MISSING, MISSING, "TOKEN_K4OIVMRPJBA3OLYBULWAYCNHGJK2"),
	1088501129639374909: (MISSING, MISSING, "TOKEN_CIPIGOZSPUQQZHUVQ9SOIS6MAH53"),
	1140389835950010520: (MISSING, MISSING, "TOKEN_173T1ODOTSXHYM1GRAIVQYDVW0I1"),
	1145489833075146772: (MISSING, MISSING, "TOKEN_NI7GCMTB8LGCLNV7H2YEJ2VUFHI1"),
	1145889544227532841: (MISSING, MISSING, "TOKEN_LLZ0V7CAZXVSVC0M1MVQCKOXCJV2"),
	1147165285623795843: (MISSING, MISSING, "TOKEN_SHDNTSTH4TPFNG0CO1LBVDANLVO2"),
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
				t = datetime.now().astimezone(timezone.utc)
				timeframes = get_accepted_timeframes(t)

				if "1m" in timeframes:
					await self.process_posts()

			except (KeyboardInterrupt, SystemExit): return
			except:
				print(format_exc())
				if environ["PRODUCTION"]: self.logging.report_exception()


	# -------------------------
	# Scheduled Posts
	# -------------------------

	async def process_posts(self):
		print("Started processing posts")
		startTimestamp = time()
		conn = TCPConnector(limit=5)
		async with ClientSession(connector=conn) as session:
			try:
				requestMap = {}
				requests = []
				guilds = database.document("details/scheduledPosts").collections()

				async for guild in guilds:
					guildId = guild.id
					if not environ["PRODUCTION"] and guildId != "926518026457739304":
						continue

					guildProperties = await self.guildProperties.get(guildId, {})
					if not guildProperties:
						guildProperties = (await database.document(f"discord/properties/guilds/{guildId}").get()).to_dict()
						if not guildProperties: guildProperties = {}
					if guildProperties.get("stale", {}).get("count", 0) > 0:
						print(f"Skipping post {guildId}/{post.id} due to stale guild")
						continue
					accountId = guildProperties.get("settings", {}).get("setup", {}).get("connection")
					userProperties = await self.accountProperties.get(accountId, {})

					async for post in guild.stream():
						data = post.to_dict()

						if data.get("timestamp", time()) < time() - 86400 * 2:
							print(f"Deleting stale post {guildId}/{post.id}")
							await post.reference.delete()
							continue

						if data["start"] > time() or int(data["start"] / 60) % data["period"] != int(time() / 60) % data["period"]: continue

						if data.get("exclude") == "outside us market hours":
							today = datetime.now().astimezone(timezone.utc)
							if today.hour < 14 or (today.hour == 14 and today.minute < 30) or today.hour > 21: continue
							yesterday = today.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
							startTime = yesterday.strftime("%Y%m%d")
							url = f"https://cloud.iexapis.com/stable/ref-data/us/dates/trade/next/1/{startTime}?token={environ['IEXC_KEY']}"
							async with session.get(url) as resp:
								if resp.status != 200: continue
								resp = await resp.json()
								if resp[0]["date"] != today.strftime("%Y-%m-%d"):
									print(f"Skipping post {guildId}/{post.id}")
									continue
						elif data.get("exclude") == "weekends":
							weekday = datetime.now().astimezone(timezone.utc).weekday()
							if weekday == 5 or weekday == 6:
								print(f"Skipping post {guildId}/{post.id}")
								continue

						if not guildProperties:
							print(f"Deleting post {guildId}/{post.id} due to missing guild")
							await post.reference.delete()

						request = CommandRequest(
							accountId=accountId,
							authorId=data["authorId"],
							channelId=data["channelId"],
							guildId=guildId,
							accountProperties=userProperties,
							guildProperties=guildProperties
						)

						if not request.scheduled_posting_available():
							print(f"Skipping post {guildId}/{post.id} due to missing subscription")
							continue

						subscriptions = sorted(userProperties["customer"]["subscriptions"].keys())
						key = f"{data['authorId']} {subscriptions} {' '.join(data['arguments'])}"
						if key in requestMap:
							print(f"Using cached response for post {guildId}/{post.id}")
							requestMap[key][1].append(len(requests))
						else:
							print(f"Creating new request for post {guildId}/{post.id}")
							requestMap[key] = [
								create_task(self.process_request(session, request, data)),
								[len(requests)]
							]
						requests.append((data, request, post))

				tasks = []
				for key, [response, indices] in requestMap.items():
					files, embeds = await response
					for i in indices:
						data, request, post = requests[i]
						print(f"Pushing post {request.guildId}/{post.id}")
						tasks.append(create_task(self.push_post(session, files, embeds, data, post.reference, request)))
				if len(tasks) > 0: await wait(tasks)

				print("Task finished in", time() - startTimestamp, "seconds")
			except (KeyboardInterrupt, SystemExit): pass
			except:
				print(format_exc())
				if environ["PRODUCTION"]: self.logging.report_exception()

	async def process_request(self, session, request, data):
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

			elif data["command"] == "layout":
				responseMessage, task = await process_chart_arguments(data["arguments"][2:], ["TradingView Relay"], tickerId=data["arguments"][1].upper(), defaults=request.guildProperties["charting"])

				if responseMessage is not None:
					description = "Detailed guide with examples is available on [our website](https://www.alpha.bot/features/layouts)."
					embed = Embed(title=responseMessage, description=description, color=constants.colors["gray"])
					embed.set_author(name="Invalid argument", icon_url=static_storage.error_icon)
					return [], [embed]

				task["TradingView Relay"]["url"] = data["arguments"][0]

				currentTask = task.get(task.get("currentPlatform"))
				timeframes = task.pop("timeframes")
				for p, t in timeframes.items(): task[p]["currentTimeframe"] = t[0]

				payload, responseMessage = await process_task(task, "chart", origin=origin)

				files, embeds = [], []
				if payload is None:
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

				return [], [embed]

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

			elif data["command"] == "lookup market-movers":
				[category, limit] = data["arguments"]

				parts = category.split(" ")
				direction = parts.pop()
				market = " ".join(parts)
				embed = Embed(title=f"Top {category}", color=constants.colors["deep purple"])

				if market == "crypto":
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
							response.append({"name": e["name"], "symbol": e["symbol"].upper(), "change": e["price_change_percentage_24h_in_currency"]})

					if direction == "gainers":
						response = sorted(response, key=lambda k: k["change"], reverse=True)[:10]
					elif direction == "losers":
						response = sorted(response, key=lambda k: k["change"])[:10]

					for token in response:
						embed.add_field(name=f"{token['name']} (`{token['symbol']}`)", value="{:+,.2f}%".format(token["change"]), inline=True)

				else:
					url = f"https://api.twelvedata.com/market_movers/{market.replace(' ', '_')}?apikey={environ['TWELVEDATA_KEY']}&direction={direction}&outputsize=10"
					async with session.get(url) as resp:
						response = await resp.json()
						for asset in response["values"]:
							embed.add_field(name=f"{asset['name']} (`{asset['symbol']}`)", value="{:+,.2f}%".format(asset["percent_change"]), inline=True)

				return [], [embed]

			else:
				raise Exception(f"invalid command: {data['command']}")

		except (KeyboardInterrupt, SystemExit): pass
		except:
			print(data["authorId"], data["channelId"])
			print(format_exc())
			if environ["PRODUCTION"]: self.logging.report_exception()
		return [], []

	async def push_post(self, session, files, embeds, data, reference, request):
		try:
			if len(files) == 0 and len(embeds) == 0:
				raise Exception("no files or embeds to send")

			botId = data.get("botId", ALPHABOT_ID)
			if botId == ALPHABOT_BETA_ID and environ["PRODUCTION"]:
				return

			name, avatar, token = BOT_CONFIG.get(botId, BOT_CONFIG[ALPHABOT_ID])

			webhooksEndpoint = f"https://discord.com/api/channels/{request.channelId}/webhooks"
			headers = {"Authorization": f"Bot {environ[token]}"}
			async with session.get(webhooksEndpoint, headers=headers) as response:
				if response.status != 200:
					raise NotFound(response, "couldn't get webhooks")
				webhooks = await response.json()
			existing = next((e for e in webhooks if e["user"]["id"] == botId), None)

			if existing is None:
				# Get bot user info
				async with session.get(f"https://discord.com/api/users/{botId}", headers=headers) as response:
					if response.status == 200:
						# Download bot icon
						botUser = await response.json()
						username = botUser["username"]
						iconUrl = f"https://cdn.discordapp.com/avatars/{botId}/{botUser['avatar']}.png?size=512"
						async with session.get(iconUrl) as response:
							botIcon = BytesIO(await response.read())
					else:
						# Use default icon
						async with session.get(iconUrl) as response:
							botIcon = BytesIO(await response.read())

				webhookData = {"name": username, "avatar": f"data:image/png;base64,{b64encode(botIcon.getvalue()).decode('utf-8')}"}
				async with session.post(webhooksEndpoint, headers=headers, json=webhookData) as response:
					if response.status != 200:
						raise NotFound(response, "failed to create webhook")
					data["url"] = (await response.json())["url"]
					await reference.update({"url": data["url"]})

			elif "url" not in existing:
				print(existing)
				raise Exception("webhook doesn't have a url")

			elif existing["url"] != data["url"]:
				data["url"] = existing["url"]
				await reference.update({"url": data["url"]})

			content = None
			if data.get("message") is not None:
				embeds.append(Embed(description=data.get("message"), color=constants.colors["purple"]))
			if data.get("tag") is not None:
				content = f"<@&{message.get('tag')}>"

			webhook = Webhook.from_url(data["url"], session=session)
			message = await webhook.send(
				content=content,
				files=files,
				embeds=embeds,
				username=name,
				avatar_url=avatar,
				wait=True
			)
			print(f"Posted message {message.id} to {request.guildId}")

			if data.get("status") == "failed":
				await reference.set({"status": DELETE_FIELD, "timestamp": DELETE_FIELD}, merge=True)
		except (KeyboardInterrupt, SystemExit): pass
		except NotFound:
			print(format_exc())
			print(f"Webhook not found in {request.guildId}")
			if data.get("status") != "failed":
				await database.document(f"discord/properties/messages/{str(uuid4())}").set({
					"title": "Scheduled post is failing!",
					"description": f"You have scheduled a post (`/{data['command']} {' '.join([for e in data['arguments'] if e != ""])}`) to be sent to a channel that no longer exists or no longer has Alpha.bot's webhook. Use `/schedule list` to review, delete and reschedule the post if you want to keep it. If the post keeps failing, it will be automatically deleted in 2 days.",
					"subtitle": "Scheduled posts",
					"color": 6765239,
					"user": data['authorId'],
					"channel": data['channelId'],
					"backupUser": data['authorId'],
					"backupChannel": data['channelId'],
					"botId": data.get("botId", "401328409499664394")
				})
				await reference.set({"status": "failed", "timestamp": time()}, merge=True)
		except:
			print(f"{request.guildId}/{data['channelId']} set by {data['authorId']}")
			print(format_exc())
			if environ["PRODUCTION"]: self.logging.report_exception()

if __name__ == "__main__":
	scheduler = Scheduler()
	print("[Startup]: Scheduler is online")
	run(scheduler.run())
