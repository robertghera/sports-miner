import { readdirSync, statSync, createReadStream } from "fs";
import { join, extname } from "path";
import csv from "csv-parser";
import { MongoClient } from "mongodb";

const uri = "mongodb://localhost:27017/";
const dbName = "sports-miner";
const collectionName = "fbref";

const dataDirectory = join("../", "DATA");
const LEAGUES_MAP = {
    E0: "Premier League",
    SP1: "La Liga",
    I1: "Serie A",
    F1: "Ligue 1",
    D1: "Bundesliga",
    P1: "Primeira Liga",
    N1: "Eredivisie",
};

// Function to recursively traverse directories and process CSV files
async function traverseAndProcess(directory) {
    const files = readdirSync(directory);
    for (const file of files) {
        const fullPath = join(directory, file);
        const stats = statSync(fullPath);
        if (stats.isDirectory()) {
            await traverseAndProcess(fullPath); // Recursive call for subdirectories
        } else if (extname(fullPath) === ".csv") {
            await processCSV(fullPath);
        }
    }
}

const TEAM_MAPPINGS = {
    Tottenham: "Tottenham Hotspur",
    Wolves: "Wolverhampton Wanderers",
    Leicester: "Leicester City",
    Hull: "Hull City",
    "M'gladbach": "Mönchengladbach",
    "Ein Frankfurt": "Eintracht Frankfurt",
    Paderborn: "Paderborn 07",
    "FC Koln": "Köln",
    Betis: "Real Betis",
    Sociedad: "Real Sociedad",
    "Ath Bilbao": "Athletic Club",
    "Paris SG": "Paris Saint-Germain",
    Setubal: "Vitória Setúbal",
    "Sp Braga": "Braga",
    Guimaraes: "Vitória Guimarães",
    "Sp Lisbon": "Sporting CP",
    Maritimo: "Marítimo",
    Famalicao: "Famalicão",
    Nijmegen: "NEC Nijmegen",
};

async function updateDatabase(records, league, season) {
    const client = new MongoClient(uri);
    try {
        await client.connect();
        const collection = client.db(dbName).collection(collectionName);

        for (const record of records) {
            let {
                date,
                HomeTeam,
                AwayTeam,
                FTHG,
                FTAG,
                HS,
                AS,
                HST,
                AST,
                HY,
                AY,
                HC,
                AC,
                HF,
                AF,
                Odds,
            } = record;
            const dateArray = date.split("/");
            let dateQuery;
            try {
                dateQuery = Date.parse(
                    (dateArray[2].length === 2
                        ? "20" + dateArray[2]
                        : dateArray[2]) +
                        "/" +
                        dateArray[1] +
                        "/" +
                        dateArray[0]
                );
            } catch (err) {
                console.log(date);
                console.log(dateArray);
                console.log(HomeTeam, AwayTeam);
                console.log(records[0]);
                process.exit(1);
            }

            if (TEAM_MAPPINGS[HomeTeam] !== undefined) {
                HomeTeam = TEAM_MAPPINGS[HomeTeam];
            }
            if (TEAM_MAPPINGS[AwayTeam] !== undefined) {
                AwayTeam = TEAM_MAPPINGS[AwayTeam];
            }

            if (
                HomeTeam === "Utrecht" &&
                AwayTeam === "Sparta Rotterdam" &&
                season === "2023-2024"
            ) {
                console.log(
                    "Utrecht vs Sparta Rotterdam - HomeTeam: ",
                    HomeTeam,
                    " AwayTeam: ",
                    AwayTeam
                );

                console.log(
                    await collection
                        .find({
                            date: dateQuery,
                            league: league,
                            season: season,
                        })
                        .toArray()
                );
                console.error("No documents found for the same match");

                console.log(
                    dateQuery,
                    league,
                    season,
                    HomeTeam,
                    AwayTeam,
                    FTHG,
                    FTAG
                );
                console.log("Shots: ", HS, AS, HST, AST);
                console.log("Fouls: ", HY, AY, HF, AF);
                console.log("Corners: ", HC, AC);
                console.log(
                    (dateArray[2].length === 2
                        ? "20" + dateArray[2]
                        : dateArray[2]) +
                        "/" +
                        dateArray[1] +
                        "/" +
                        dateArray[0]
                );
            }

            let doc = await collection
                .find({
                    date: dateQuery,
                    league: league,
                    season: season,
                    home_team: HomeTeam,
                })
                .toArray();

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        away_team: AwayTeam,
                    })
                    .toArray();
            }

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        "stats.score_home": Number(FTHG),
                        "stats.score_away": Number(FTAG),
                        "stats.shots_home": Number(HS),
                        "stats.shots_away": Number(AS),
                        "stats.shots_on_target_home": Number(HST),
                    })
                    .toArray();
            }

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        "stats.score_home": Number(FTHG),
                        "stats.score_away": Number(FTAG),
                        "stats.shots_home": Number(HS),
                        "stats.shots_away": Number(AS),
                        "stats.shots_on_target_away": Number(AST),
                    })
                    .toArray();
            }

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        "stats.score_home": Number(FTHG),
                        "stats.score_away": Number(FTAG),
                        "stats.fouls_home": Number(HF),
                        "stats.fouls_away": Number(AF),
                        "stats.cards_yellow_home": Number(HY),
                        "stats.cards_yellow_away": Number(AY),
                    })
                    .toArray();
            }

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        "stats.score_home": Number(FTHG),
                        "stats.score_away": Number(FTAG),
                        "stats.corners_home": Number(HC),
                        "stats.corners_away": Number(AC),
                        "stats.cards_yellow_home": Number(HY),
                        "stats.cards_yellow_away": Number(AY),
                    })
                    .toArray();
            }

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        "stats.score_home": Number(FTHG),
                        "stats.score_away": Number(FTAG),
                        "stats.corners_home": Number(HC),
                        "stats.corners_away": Number(AC),
                        "stats.fouls_home": Number(HF),
                        "stats.fouls_away": Number(AF),
                    })
                    .toArray();
            }

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        "stats.score_home": Number(FTHG),
                        "stats.score_away": Number(FTAG),
                        "stats.corners_home": Number(HC),
                        "stats.shots_home": Number(HS),
                        "stats.shots_on_target_home": Number(HST),
                        "stats.cards_yellow_home": Number(HY),
                        "stats.fouls_home": Number(HF),
                    })
                    .toArray();
            }

            if (doc.length === 0) {
                doc = await collection
                    .find({
                        date: dateQuery,
                        league: league,
                        season: season,
                        "stats.score_home": Number(FTHG),
                        "stats.score_away": Number(FTAG),
                        "stats.corners_away": Number(AC),
                        "stats.shots_away": Number(AS),
                        "stats.shots_on_target_away": Number(AST),
                        "stats.cards_yellow_away": Number(AY),
                        "stats.fouls_away": Number(AF),
                    })
                    .toArray();
            }

            if (doc.length > 1) {
                console.error("Multiple documents found for the same match");
                console.log(doc);
                console.log(
                    dateQuery,
                    league,
                    season,
                    HomeTeam,
                    AwayTeam,
                    FTHG,
                    FTAG
                );
                console.log("Shots: ", HS, AS, HST, AST);
                console.log("Fouls: ", HY, AY, HF, AF);
                console.log("Corners: ", HC, AC);
                process.exit(1);
            }

            if (doc.length === 0) {
                console.log(
                    await collection
                        .find({
                            date: dateQuery,
                            league: league,
                            season: season,
                        })
                        .toArray()
                );
                console.error("No documents found for the same match");
                console.log(doc);
                console.log(
                    dateQuery,
                    league,
                    season,
                    HomeTeam,
                    AwayTeam,
                    FTHG,
                    FTAG
                );
                console.log("Shots: ", HS, AS, HST, AST);
                console.log("Fouls: ", HY, AY, HF, AF);
                console.log("Corners: ", HC, AC);
                console.log(
                    (dateArray[2].length === 2
                        ? "20" + dateArray[2]
                        : dateArray[2]) +
                        "/" +
                        dateArray[1] +
                        "/" +
                        dateArray[0]
                );
                process.exit(1);
            }

            await collection.updateOne(
                { m: doc[0].m },
                { $set: { odds: Odds } }
            );
        }
    } catch (err) {
        console.error("Error updating the database:", err);
    } finally {
        await client.close();
    }
}

// Function to process a single CSV file
async function processCSV(filePath) {
    const records = [];
    const filePathParts = filePath.split("\\").pop().split("-");
    const season =
        "20" + filePathParts[1] + "-" + "20" + filePathParts[2].split(".")[0];
    const league = LEAGUES_MAP[filePathParts[0]];
    createReadStream(filePath)
        .pipe(csv())
        .on("data", (row) => {
            // Extract relevant fields from the CSV row
            const {
                Date,
                HomeTeam,
                AwayTeam,
                FTHG,
                FTAG,
                HS,
                AS,
                HST,
                AST,
                HY,
                AY,
                HC,
                AC,
                HF,
                AF,
                B365H,
                B365D,
                B365A,
                BWH,
                BWD,
                BWA,
                IWH,
                IWD,
                IWA,
            } = row;

            const date = Date;

            // Create a record object with the extracted data
            const record = {
                date,
                HomeTeam,
                AwayTeam,
                FTHG,
                FTAG,
                HS,
                AS,
                HST,
                AST,
                HY,
                AY,
                HC,
                AC,
                HF,
                AF,
                Odds: {
                    B365H: parseFloat(B365H),
                    B365D: parseFloat(B365D),
                    B365A: parseFloat(B365A),
                    BWH: parseFloat(BWH),
                    BWD: parseFloat(BWD),
                    BWA: parseFloat(BWA),
                    IWH: parseFloat(IWH),
                    IWD: parseFloat(IWD),
                    IWA: parseFloat(IWA),
                },
            };
            records.push(record);
        })
        .on("end", async () => {
            console.log(`Finished processing ${filePath}`);
            await updateDatabase(records, league, season);
        });
}

try {
    await traverseAndProcess(dataDirectory);
} catch (err) {
    console.error("Error processing files:", err);
}
