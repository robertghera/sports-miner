import { join } from "path";
import { parse } from "csv-parse";
import { MongoClient } from "mongodb";

const uri = `mongodb+srv://${process.env.database_username}:${process.env.database_password}@cluster.ywrlr.mongodb.net/score-lab?retryWrites=true&w=majority&appName=Cluster`;
const dbName = "sports-miner";
const collectionName = "fbref";

const SEASON = "2024-2025";

const LINKS = [
    ["https://www.football-data.co.uk/mmz4281/2425/E0.csv", "Premier League"],
    ["https://www.football-data.co.uk/mmz4281/2425/SP1.csv", "La Liga"],
    ["https://www.football-data.co.uk/mmz4281/2425/I1.csv", "Serie A"],
    ["https://www.football-data.co.uk/mmz4281/2425/F1.csv", "Ligue 1"],
    ["https://www.football-data.co.uk/mmz4281/2425/D1.csv", "Bundesliga"],
    ["https://www.football-data.co.uk/mmz4281/2425/N1.csv", "Eredivisie"],
    ["https://www.football-data.co.uk/mmz4281/2425/P1.csv", "Primeira Liga"],
];

// Function to recursively traverse directories and process CSV files
async function traverseAndProcess() {
    for (const [link, league] of LINKS) {
        const response = await fetch(link);
        console.log(league, SEASON);
        await processCSV(response, league, SEASON);
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
    AVS: "AVS Futebol",
    "Gil Vicente": "Gil Vicente FC",
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

            // console.log(HomeTeam, AwayTeam, date, FTHG, FTAG, Odds);

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
                continue;
                // process.exit(1);
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
async function processCSV(response, league, season) {
    const records = [];
    const text = await response.text();

    parse(
        text,
        {
            columns: true, // Assuming your CSV has headers
            skip_empty_lines: true,
        },
        async (err, output) => {
            if (err) {
                console.error("Error parsing CSV:", err);
                return;
            }

            output.forEach((row) => {
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
            });
            await updateDatabase(records, league, season);
        }
    );
}

try {
    await traverseAndProcess();
} catch (err) {
    console.error("Error processing:", err);
}
