export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function removePercentageFromArray(arr) {
    return arr.map((el) => el.replace("%", ""));
}

export function formatPassesArray(arr) {
    return [
        ...arr[0]
            .replaceAll(" ", "")
            .replaceAll(" ", "")
            .split("—")[0]
            .split("of"),
        ...arr[1]
            .replaceAll(" ", "")
            .replaceAll(" ", "")
            .split("—")[1]
            .split("of"),
    ];
}

export function formatDocument(doc, newFormatStats) {
    const finalDoc = {};
    let totalNormalStats = 0;
    Object.keys(doc).forEach((key) => {
        if (doc[key] === null) {
            delete doc[key];
        }
        if (Array.isArray(doc[key])) {
            if (
                key !== null &&
                (newFormatStats.includes(key) || key === "score_xg")
            ) {
                if (finalDoc.extraStats === undefined) {
                    finalDoc.extraStats = {};
                }
                finalDoc.extraStats[key + "_home"] = Number(doc[key][0]);
                finalDoc.extraStats[key + "_away"] = Number(doc[key][1]);
            } else {
                if (finalDoc.stats === undefined) {
                    finalDoc.stats = {};
                }
                finalDoc.stats[key + "_home"] = Number(doc[key][0]);
                finalDoc.stats[key + "_away"] = Number(doc[key][1]);
                totalNormalStats += 1;
            }
        } else {
            finalDoc[key] = doc[key];
        }
    });

    if (totalNormalStats < 12) {
        console.log("Not enough stats: ", doc.m);
    }
    return finalDoc;
}

export function formatLeague(title) {
    let league = title.split(" ");
    const season = league.shift();
    league = league.join(" ").split(" Scores & Fixtures")[0].replace("\n", "");
    return [league, season];
}
