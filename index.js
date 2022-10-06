const axios = require("axios");
const { NFTStorage } = require("nft.storage");
const { createClient } = require("@supabase/supabase-js");
const express = require("express");
require("dotenv").config();

const app = express();

const db = createClient(process.env.DB_URL_IPFS, process.env.DB_URL_IPFS_GATE);
const client = new NFTStorage({ token: process.env.IPFS_STORAGE });

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
  const r = await fetch(_original);
  const blobFile = await r.blob();
  if (!r.ok) {
    await catchErr();
    throw new Error(`error fetching image: [${r.statusCode}]: ${r.status}`);
  } else {
    const cid = await client.storeBlob(blobFile);
    return cid;
  }
}

async function catchErr() {
  const { data, error: errors } = await db
    .from("count")
    .upsert({ running: false })
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
      console.log({ errorSupa: error });
    } else {
      console.log({ data });
    }
  } catch (error) {
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
    console.log({ error });
  }
}

async function main(_url) {
  try {
    const { next, results } = await fetchRes(_url);
    console.log({ next });
    if (!next) {
      console.log("finish");
    } else {
      await getArr(results);
      const { data, error } = await db
        .from("count")
        .upsert({ running: true, url: next })
        .match({ id: 1 });
      if (error) {
        console.log("error upsert", error);
      }
      if (data) {
        console.log({ next });
      }
      await main(next);
    }
  } catch (error) {
    console.log(error);
  }
}

async function getArr(result) {
  try {
    const imageIds = result.map((item) => item.image_id);
    console.log({ imageIds });
    const details = await Promise.all(
      imageIds.map(async (file) => {
        const contents = await axios(`${process.env.DETAIL_ORIGIN}${file}`);
        return contents.data;
      })
    );
    console.log("uploading...");
    for (const detail of details) {
      await upload(detail);
    }
  } catch (error) {
    console.log({ error });
  }
}

const statusResp = (res, _code, _resp) => {
  res.status(_code).json(_resp);
};

app.get("/", async (req, res) => {
  statusResp(res, 200, { message: "running" });
  const { url } = await fetchCount();
  if (!url) {
    await main(process.env.URL_ORIGIN);
  } else {
    console.log("fetching..");
    await main(url);
  }
});

app.get("/count", async (req, res) => {
  const data = await fetchCount();
  statusResp(res, 200, { data });
});

app.listen(8000, async () => {
  console.log("listening at port 8000");
});
