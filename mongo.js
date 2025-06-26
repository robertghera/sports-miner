import { MongoClient } from "mongodb";
const MONGO_URL = "mongodb://localhost:27017";
const client = new MongoClient(MONGO_URL);
await client.connect();
const database = client.db("sports-miner");
const info = database.collection("info");

const season = "2019-2020";
const docs = await info
    .find({})
    .skip(380 * 4)
    .limit(380)
    .toArray();
console.log(docs[0]);

for (const doc of docs) {
    Object.keys(doc).forEach((key) => {
        if (key.endsWith("_home") || key.endsWith("_away")) {
            doc[key] = Number(doc[key]);
        }
    });
    doc.season = season;
    await info.updateOne({ m: doc.m }, { $set: doc });
}

process.exit(0);
