const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");

// Connection URL
const url = "mongodb://localhost:27017";
const dbName = "tradesdb";
const collectionName = "trades";

const client = new MongoClient(url, { useNewUrlParser: true });

(async function() {
  try {
    await client.connect();
    console.info("Connected to mongo server correctly!");
    const db = client.db(dbName);
    const col = db.collection(collectionName);

    doCollectionClone(client, db, col);
    // doCollectionCloneTwo(client, db, col);
  } catch (error) {
    console.error("CONNECTION ERROR::", error.stack);
  }
})();

//RIGHT WAY
//takes around 403MB of memeory and completes in ~18secs for 1 Million records
//Ofcourse the memory cosumption is still large because we have to keep interval-data in memory
//For writable srteam like (write data to file) stream.pipe(writable) would do magic & memory consumption would not go beyond 150MB
function doCollectionClone(client, db, col) {
  console.time("CLONING");

  const newCol = db.collection("tradesClone");
  const stream = col.find().stream();
  let bulkOp = [];
  let bulkOpPromises = [];

  stream.on("data", function(data) {
    bulkOp.push({
      insertOne: {
        document: data
      }
    });
    if (bulkOp.length > 8000) {
      const p = newCol.bulkWrite(bulkOp, { ordered: false, w: 1 });
      bulkOpPromises.push(p);
      bulkOp = [];
    }
  });

  stream.on("error", function(error) {
    console.log("STREAM ERROR::", error.stack);
  });

  stream.on("end", function() {
    console.log("FINISHED");
    if (bulkOp.length > 0) {
      const p = newCol.bulkWrite(bulkOp, { ordered: false, w: 1 });
      bulkOpPromises.push(p);
      bulkOp = [];
    }

    Promise.all(bulkOpPromises)
      .then(res => {
        client.close();
        console.timeEnd("CLONING");
      })
      .catch(e => {
        console.error("BULK INSERT ERROR::", e);
        client.close();
      });
  });
}

//NOT SO RIGHT FOR LARGE COLLECTIONS
//takes a whopping 1.5Gigs of memory and completes in ~35secs for 1 Million records
//Had this memory reached > 1.6 you'll get a nice-long error JS heap out of memory and app will crash
async function doCollectionCloneTwo(client, db, col) {
  console.time("CLONING");
  try {
    //1.Get clone collection
    const newCol = db.collection("tradesClone");

    //2.Get the docs and do bulk
    const docs = await col.find({}).toArray();
    const len = docs.length;
    const p = await newCol.insertMany(docs, { w: 1 });

    //3.Check and close
    assert.equal(len, p.insertedCount);
    client.close();
    console.timeEnd("CLONING");
  } catch (error) {
    console.error("AT doCollectionCloneTwo()::", error);
    client.close();
  }
}
