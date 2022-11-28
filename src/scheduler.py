from os import environ
environ["PRODUCTION"] = environ["PRODUCTION"] if "PRODUCTION" in environ and environ["PRODUCTION"] else ""

from signal import signal, SIGINT, SIGTERM
from time import time
from random import randint
from datetime import datetime
from aiohttp import TCPConnector, ClientSession
from asyncio import sleep, wait, run, gather, create_task
from pytz import utc
from traceback import format_exc

from discord import Webhook, Embed, File
from google.cloud.firestore import AsyncClient as FirestoreClient
from google.cloud.error_reporting import Client as ErrorReportingClient

from helpers import constants
from assets import static_storage
from Processor import process_chart_arguments, process_heatmap_arguments, process_task
from DatabaseConnector import DatabaseConnector
from CommandRequest import CommandRequest
from helpers.utils import seconds_until_cycle, get_accepted_timeframes


database = FirestoreClient()


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

					async for post in guild.stream():
						data = post.to_dict()
						if data["start"] > time() or int(data["start"] / 60) % data["period"] != int(time() / 60) % data["period"]: continue

						if data.get("exclude") == "outside market hours":
							today = datetime.now().astimezone(utc)
							if today.hour < 14 or (today.hour == 14 and today.minute < 30) or today.hour > 21: continue
							yesterday = today.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
							startTime = yesterday.strftime("%Y%m%d")
							url = f"https://cloud.iexapis.com/stable/ref-data/us/dates/trade/next/1/{startTime}?token={environ['IEXC_KEY']}"
							async with session.get() as resp:
								if resp.status != 200: continue
								data = await resp.json()
								if data[0]["date"] != today.strftime("%Y-%m-%d"): continue
						elif data.get("exclude") == "weekends":
							weekday = datetime.now().astimezone(utc).weekday()
							if weekday == 5 or weekday == 6: continue

						[accountId, user, guild] = await gather(
							self.accountProperties.match(data["authorId"]),
							self.accountProperties.get(str(data["authorId"]), {}),
							self.guildProperties.get(guildId, {})
						)

						if not guild: await post.reference.delete()
						if guild.get("stale", {}).get("count", 0) > 0: continue

						request = CommandRequest(
							accountId=accountId,
							authorId=data["authorId"],
							channelId=data["channelId"],
							guildId=guildId,
							accountProperties=user,
							guildProperties=guild
						)

						if not request.scheduled_posting_available(): continue

						subscriptions = sorted(user["customer"]["subscriptions"].keys())
						key = f"{data['authorId']} {subscriptions} {' '.join(data['arguments'])}"
						if key in requestMap:
							requestMap[key][1].append(len(requests))
						else:
							requestMap[key] = [
								create_task(self.process_request(request, data)),
								[len(requests)]
							]
						requests.append(data)

				tasks = []
				for key, [request, indices] in requestMap.items():
					files, embeds = await request
					for i in indices:
						data = requests[i]
						tasks.append(create_task(self.push_post(session, files, embeds, data)))
				if len(tasks) > 0: await wait(tasks)

				print("Task finished in", time() - startTimestamp, "seconds")
			except (KeyboardInterrupt, SystemExit): pass
			except Exception:
				print(format_exc())
				if environ["PRODUCTION"]: self.logging.report_exception()

	async def process_request(self, request, data):
		try:
			if data["command"] == "chart":
				platforms = request.get_platform_order_for("c")
				responseMessage, task = await process_chart_arguments(data["arguments"][1:], platforms, tickerId=data["arguments"][0].upper(), defaults=request.guildProperties["charting"])

				if responseMessage is not None:
					description = "[Advanced Charting add-on](https://www.alpha.bot/pro/advanced-charting) unlocks additional assets, indicators, timeframes and more." if responseMessage.endswith("add-on.") else "Detailed guide with examples is available on [our website](https://www.alpha.bot/features/charting)."
					embed = Embed(title=responseMessage, description=description, color=constants.colors["gray"])
					embed.set_author(name="Invalid argument", icon_url=static_storage.icon_bw)
					try: await ctx.interaction.edit_original_response(embed=embed)
					except NotFound: pass
					return

				currentTask = task.get(task.get("currentPlatform"))
				timeframes = task.pop("timeframes")
				for p, t in timeframes.items(): task[p]["currentTimeframe"] = t[0]

				payload, responseMessage = await process_task(task, "chart")

				files, embeds = [], []
				if responseMessage == "requires pro":
					embed = Embed(title=f"The requested chart for `{currentTask.get('ticker').get('name')}` is only available on TradingView Premium.", description="All TradingView Premium charts are bundled with the [Advanced Charting add-on](https://www.alpha.bot/pro/advanced-charting).", color=constants.colors["gray"])
					embed.set_author(name="TradingView Premium", icon_url=static_storage.icon_bw)
					embeds.append(embed)
				elif payload is None:
					errorMessage = f"Requested chart for `{currentTask.get('ticker').get('name')}` is not available." if responseMessage is None else responseMessage
					embed = Embed(title=errorMessage, color=constants.colors["gray"])
					embed.set_author(name="Chart not available", icon_url=static_storage.icon_bw)
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
					embed.set_author(name="Invalid argument", icon_url=static_storage.icon_bw)
					try: await ctx.interaction.edit_original_response(embed=embed)
					except NotFound: pass
					return

				currentTask = task.get(task.get("currentPlatform"))
				timeframes = task.pop("timeframes")
				for p, t in timeframes.items(): task[p]["currentTimeframe"] = t[0]

				payload, responseMessage = await process_task(task, "heatmap")

				files, embeds = [], []
				if payload is None:
					errorMessage = "Requested heatmap is not available." if responseMessage is None else responseMessage
					embed = Embed(title=errorMessage, color=constants.colors["gray"])
					embed.set_author(name="Heatmap not available", icon_url=static_storage.icon_bw)
					embeds.append(embed)
				else:
					files.append(File(payload.get("data"), filename="{:.0f}-{}-{}.png".format(time() * 1000, request.authorId, randint(1000, 9999))))
				
				return files, embeds

		except (KeyboardInterrupt, SystemExit): pass
		except Exception:
			print(format_exc())
			if environ["PRODUCTION"]: self.logging.report_exception()

	async def push_post(self, session, files, embeds, data):
		content = None
		if data.get("message") is not None:
			embeds.append(Embed(description=data.get("message"), color=constants.colors["purple"]))
		if data.get("tag") is not None:
			content = f"<@&{message.get('tag')}>"

		await Webhook.from_url(data["url"], session=session).send(
			content=content,
			files=files,
			embeds=embeds,
			username="Alpha",
			avatar_url="https://cdn.discordapp.com/app-icons/401328409499664394/326e5bef971f8227de79c09d82031dda.png",
			wait=False
		)

if __name__ == "__main__":
	scheduler = Scheduler()
	run(scheduler.run())
