// import puppeteer from "puppeteer";
import { MongoClient } from "mongodb";
import * as utils from "./utils.js";
import config from "./config.json" with {type: "json"};
import md5 from "md5";
import puppeteer from 'puppeteer-extra';
import StealthPlugin  from 'puppeteer-extra-plugin-stealth';

const DOMAIN = "https://fbref.com";
const MONGO_URL = `mongodb+srv://${process.env.DATABASE_USERNAME}:${process.env.DATABASE_PASSWORD}@cluster.ywrlr.mongodb.net/score-lab?retryWrites=true&w=majority&appName=Cluster`;

const client = new MongoClient(MONGO_URL);
await client.connect();
const database = client.db("sports-miner");
const info = database.collection("fbref");

puppeteer.use(StealthPlugin());
const browser = await puppeteer.launch({
    headless: config.headless,
    args: ['--no-sandbox']
});
const page = await browser.newPage();
await page.setViewport({ width: 1300, height: 1300 });
page.setDefaultNavigationTimeout(30000);
const ua =
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36";
await page.setUserAgent(ua);

// parse all leagues in config
let totalGames = 0;

try {
    for (const pageUrl of config.leaguesUrl) {
        await page.goto(pageUrl, {waitUntil: "domcontentloaded"});
        console.log("loaded page");
        if (totalGames > 0) {
            totalGames = 0;
        }

        for (let i = 0; i < config.seasonToIterate; i++) {
            console.log("Season", i + 1);
            if (!config.currentSeason) {
                await page.waitForSelector(".prevnext a");
                await page.click(".prevnext a");
            }

            // get season
            await page.waitForSelector("#meta h1");
            const title = await page.$eval("#meta h1", (n) => n.innerText);
            const [league, season] = utils.formatLeague(title);
            console.log(season);
            console.log(league);

            try {
                await page.waitForSelector(".filter a", {timeout: 10000});
                await page.$$eval(".filter a", (nodes) => nodes[1].click());
            } catch (e) {
                await page.waitForSelector(`th[data-stat="date"]`);
                await page.click(`th[data-stat="date"]`);
                console.log("No filter button found")
            }
            // Extract all the values of the specified data-stat column
            await page.waitForSelector(`td[data-stat="score"]`);
            const columnData = await page.$$eval(
                `div.current td[data-stat="score"] a`,
                (elements) => elements.map((el) => el.getAttribute("href"))
            );

            if (config.currentSeason) {
                const value = await info.find({season: season, league: league}).toArray();
                console.log("Past games already parsed: " + value.length)
                if (value.length > 0) {
                    config.skip = value.length;
                }
                // totalGames = 0; // TODO: maybe find a nicer way to do this
            }

            const pageUrl = page.url();
            // parse all games in the league season
            for (const element of columnData) {
                if (config.skip > 0 && totalGames < config.skip) {
                    totalGames += 1;
                    // console.log(totalGames)
                    continue; 
                }
                for (const retry of [1, 2, 3]) {
                    if (retry > 1) {
                        console.log(`Retry: ${retry}`);
                    }
                    try {
                        await page.goto(DOMAIN + element, {waitUntil: "domcontentloaded"});
                        await page.waitForSelector("div.scorebox strong a");
                        break;
                    } catch (e) {
                        console.log(e);
                        console.log("Error loading page, retrying...");
                        if (retry === 3) {
                            console.log("Failed to load page after 3 retries, stopping...");
                            throw e;
                        }
                        await utils.sleep(3000 * retry);
                        await page.reload();
                    }
                }
                // also last element in the array is the date maybe find a nicer way
                const teams = await page.$$eval("div.scorebox strong a", (nodes) =>
                    nodes.map((n) => n.innerText)
                );
                console.log(teams);

                const score = await page.$$eval(".scorebox .scores .score", (nodes) =>
                    nodes.map((n) => n.innerText)
                );
                const score_xg = await page.$$eval(".scorebox .scores .score_xg", (nodes) =>
                    nodes.map((n) => n.innerText)
                );

                const time = await page.$eval(".venuetime", (n) => n.getAttribute("data-venue-epoch"));

                let newFormat = true
                if (score_xg.length === 0) {
                    newFormat = false
                }

                // get possesion and passes from graphic
                await page.waitForSelector("#team_stats tr");
                let possesionHeader = await page.$$eval(
                    "#team_stats tr:nth-child(2) th",
                    (nodes) => nodes.map((n) => n.innerText)
                );
                let possesion
                if (possesionHeader[0] === "Possession") {
                    possesion = await page.$$eval(
                        "#team_stats tr:nth-child(3) td",
                        (nodes) => nodes.map((n) => n.innerText)
                    );
                    possesion = utils.removePercentageFromArray(possesion);
                } else {
                    console.log("NO POSSESION")
                    console.log(totalGames)
                    process.exit(-1)
                }
                
                const passingAccuracyHeader = await page.$$eval(
                    "#team_stats tr:nth-child(4) th",
                    (nodes) => nodes.map((n) => n.innerText)
                );
                let passes
                if (passingAccuracyHeader[0] === "Passing Accuracy") {
                    passes = await page.$$eval(
                        "#team_stats tr:nth-child(5) td",
                        (nodes) => nodes.map((n) => n.innerText)
                    );
                    passes = utils.formatPassesArray(passes);
                } else {
                    console.log("NO PASSING")
                    // console.log(totalGames)
                    // process.exit(-1)
                }
                

                // get stats from extra stats
                await page.waitForSelector("#team_stats_extra");
                let extraStats = await page.$$eval("#team_stats_extra div div", (nodes) => 
                    nodes
                    .filter((n) => !n.classList.contains("th"))
                    .map((n) => n.innerText)
                );
                const extraStatsObj = {};
                for (let i = 0; i < extraStats.length; i++) {
                    if (extraStats[i].match(/[A-Za-z]+/g)) {
                        const key = extraStats[i].toLowerCase().replace(" ", "_");
                        extraStatsObj[key] = [];
                        extraStatsObj[key].push(extraStats[i - 1]);
                        extraStatsObj[key].push(extraStats[i + 1]);
                    }
                }

                // get missing stats from table
                // TODO: for newer league versions here are more stats
                await page.waitForSelector('[id^="all_player_stats_"] tfoot');

                const missingStats = [
                    "cards_yellow",
                    "cards_red",
                    "shots",
                    "shots_on_target",
                ];

                const newFormatStats = [
                    "sca",
                    "gca",
                    "progressive_passes",
                    "carries",
                    "progressive_carries",
                    "take_ons",
                    "take_ons_won"
                ]

                if (newFormat) {
                    for (const stat of newFormatStats) {
                        missingStats.push(stat);
                    }
                }   

                for (const stat of missingStats) {
                    const statsArray = await page.$$eval(
                        `[id^="all_player_stats_"] tfoot td[data-stat="${stat}"]`,
                        (nodes) => nodes.map((n) => n.innerText)
                    );
                    if (statsArray.length > 2) {
                        extraStatsObj[stat] = [statsArray[0], statsArray[2]];

                        if (statsArray[0] !== statsArray[1]) {
                            console.log(teams);
                            console.log(stat);
                            console.log(statsArray);
                        }
                    } else {
                        extraStatsObj[stat] = statsArray;
                    }
                }

                const toInsert = {
                    m: md5(teams[0] + teams[1] + teams[2]),
                    home_team: teams[0],
                    away_team: teams[1],
                    date: new Date(teams[2]).getTime(),
                    time: Number(time) * 1000,
                    season: season,
                    league: league,
                    possesion: possesion,
                    score: score,
                    ...extraStatsObj,
                };

                if (score_xg.length > 0) {
                    toInsert.score_xg = score_xg;
                }
                if (passes && passes.length > 0) {
                    toInsert.passes = [passes[1], passes[3]];
                    toInsert.passes_completed = [passes[0], passes[2]];
                }    

                // TODO: temporary solution
                try {
                    await info.insertOne(utils.formatDocument(toInsert, newFormatStats));
                } catch (e) {
                    console.log(e);
                    if (!config.currentSeason) {
                        throw e;
                    }
                }
                
                totalGames += 1;
                await utils.sleep(Math.random() * 8000 + 1000);
            }
            
            if (page.url() !== pageUrl) {
                console.log("Changed page");
                await page.goto(pageUrl, {waitUntil: "domcontentloaded"});
            }
        }
    }
} catch (e) {
    console.log(e);
    console.log(totalGames)
}


await browser.close();
process.exit(0);