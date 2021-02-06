const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");

// Connection URL
const url = "mongodb://localhost:27017";
const dbName = "tradesdb";
const collectionName = "trades";

const client = new MongoClient(url, { useNewUrlParser: true });

(async function () {
  try {
    await client.connect();
    console.info("Connected to mongo server correctly!");
    const db = client.db(dbName);
    const col = db.collection(collectionName);

    doCollectionClone(client, db, col);
    // doCollectionCloneTwo(client, db, col);
  } catch (error) {
    console.error(`Connection error::${error}`);
  }
})();

/**
 * Reading data using streams and dumping into mongodb
 * @param {Object} client - client instance for mongodb connection.
 * @param {Object} db - database instance to use.
 * @param {string} col - collection name.
 */
function doCollectionClone(client, db, col) {
  console.time("CLONING");

  const newCol = db.collection("tradesClone");
  const stream = col.find().stream();
  let bulkOp = [];
  let bulkOpPromises = [];

  stream.on("data", function (data) {
    bulkOp.push({
      insertOne: {
        document: data,
      },
    });
    if (bulkOp.length > 8000) {
      const p = newCol.bulkWrite(bulkOp, { ordered: false, w: 1 });
      bulkOpPromises.push(p);
      bulkOp = [];
    }
  });

  stream.on("error", function (error) {
    console.log(`stream error::${error}`);
  });

  stream.on("end", function () {
    console.log("FINISHED");
    if (bulkOp.length > 0) {
      const p = newCol.bulkWrite(bulkOp, { ordered: false, w: 1 });
      bulkOpPromises.push(p);
      bulkOp = [];
    }

    Promise.all(bulkOpPromises)
      .then((res) => {
        client.close();
        console.timeEnd("CLONING");
      })
      .catch((e) => {
        console.error(`bluk insert error::${e}`);
        client.close();
      });
  });
}

/**
 *
 * @param {Object} client - client instance for mongodb connection.
 * @param {Object} db - database instance to use.
 * @param {string} col - collection name.
 */
async function doCollectionCloneTwo(client, db, col) {
  console.time("CLONING");
  try {
    const newCol = db.collection("tradesClone");

    const docs = await col.find({}).toArray();
    const len = docs.length;
    const p = await newCol.insertMany(docs, { w: 1 });

    //3.Check and close
    assert.strictEqual(len, p.insertedCount);
    client.close();
    console.timeEnd("CLONING");
  } catch (error) {
    console.error(`Error at doCollectionCloneTwo()::${error}`);
    client.close();
  }
}
