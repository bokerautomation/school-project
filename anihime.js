import axios from "axios";
import { NFTStorage } from "nft.storage";
import { createClient } from "@supabase/supabase-js";
import express from "express";
import * as dotenv from "dotenv";
import fetch from "node-fetch";

dotenv.config();

const app = express();

const db = createClient(
  process.env["DB_URL_IPFS"],
  process.env["DB_URL_IPFS_GATE"]
);
const client = new NFTStorage({ token: process.env["IPFS_STORAGE"] });

const fetchCount = async () => {
  const { data, error } = await db.from("count").select("*").match({ id: 1 });
  console.log({ data });
  if (error) {
    console.log("error fetch count");
  } else {
    return data;
  }
};

async function getExampleImage(_original) {
  console.log("exampleBlob");
  try {
    const r = await fetch(_original);
    const blobFile = await r.blob();
    if (!r.ok) {
      await catchErr();
      throw new Error(`error fetching image: [${r.statusCode}]: ${r.status}`);
    } else {
      const cid = await client.storeBlob(blobFile);
      return cid;
    }
  } catch (err) {
    await catchErr();
    console.log(err);
  }
}

async function catchErr() {
  const { data, error: errors } = await db
    .from("count")
    .update({ running: false })
    .match({ id: 1 });
  if (errors) {
    console.log("error upsert", errors);
  }
  if (data) {
    console.log({ data });
  }
}

async function upload(_detail) {
  const {
    id,
    image_id,
    creator_name,
    creator_username,
    source,
    original_image,
    is_nsfw,
  } = _detail;
  try {
    const cid = await getExampleImage(original_image);
    console.log({ cid });
    const { data, error } = await db.from("animedb").insert({
      ipfslink: `ipfs://${cid}`,
      id,
      image_id,
      creator_name,
      creator_username,
      source,
      original_image,
      is_nsfw,
    });

    if (error) {
      await catchErr();
      console.log({ errorSupa: error });
    } else {
      console.log({ data });
    }
  } catch (error) {
    await catchErr();
    console.log({ error });
  }
}

async function fetchRes(_url) {
  try {
    const dataArr = await axios(_url);
    const { next, results } = dataArr.data;
    const res = {
      next,
      results,
    };
    return res;
  } catch (error) {
    await catchErr();
    console.log({ error });
  }
}

async function concatArr(result) {
  console.log({ result });
  try {
    let groupRes = result.map((res) => res.image_id);
    const orDB = result.map((item) => "image_id.eq." + item.image_id);
    const joined = orDB.join(",");
    const { data } = await db.from("animedb").select("image_id").or(joined);
    if (!data) {
      return console.log({ data });
    } else {
      const mapped = data.map((item) => item.image_id);

      mapped.forEach((item) => {
        groupRes = groupRes.filter((elem) => item !== elem);
      });

      console.log({ after: groupRes.length });

      return groupRes;
    }
  } catch (error) {
    console.log(error);
  }
}

async function getMain(_res) {
  try {
    const { results } = await fetchRes(_res);

    const arr = await concatArr(results);
    if (!arr) {
      return console.log(arr);
    } else {
      await getArr(arr);
    }
  } catch (err) {
    console.log(err);
  }
}

async function main(_url) {
  try {
    const { next, results } = await fetchRes(_url);
    console.log({ results });
    if (!results) {
      console.log("finish");
    } else {
      const arr = await concatArr(results);
      await getArr(arr);

      const { data, error } = await db
        .from("count")
        .update({ running: false, url: next })
        .match({ id: 1 });
      if (error) {
        console.log("error upsert", error);
      }
      if (data) {
        console.log({ next });
      }
    }
  } catch (error) {
    await catchErr();
    console.log(error);
  }
}

async function getArr(result) {
  try {
    console.log({ arrLen: result.length });
    const details = await Promise.all(
      result.map(async (file) => {
        const contents = await axios(`${process.env["DETAIL_ORIGIN"]}${file}`);
        return contents.data;
      })
    );
    console.log("uploading...");
    for (const detail of details) {
      await upload(detail);
    }
  } catch (error) {
    await catchErr();
    console.log({ error });
  }
}

const statusResp = (res, _code, _resp) => {
  res.status(_code).json(_resp);
};

app.get("/", async (req, res) => {
  const supa = await fetchCount();
  const { url, running } = supa[0];
  if (!url) {
    return statusResp(res, 200, { message: "finished" });
  }
  if (running) {
    await catchErr();
    statusResp(res, 200, { running });
  } else {
    const { error } = await db
      .from("count")
      .update({ running: true })
      .match({ id: 1 });
    statusResp(res, 200, { running: "init" });
    if (error) {
      console.log(error);
    }
    await main(url);
  }
});

app.get("/start", async (req, res) => {
  const reqs = process.env["DAILY_TASK"];
  console.log({ reqs });
  await getMain(reqs);
  statusResp(res, 200, { message: "finish" });
});

app.get("/count", async (req, res) => {
  const data = await fetchCount();
  statusResp(res, 200, { data });
});

app.listen(8000, async () => {
  console.log("listening at port 8000");
});
