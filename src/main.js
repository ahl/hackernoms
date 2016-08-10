// @flow

import firebase from 'firebase';
import async from 'async';
import { newStruct, Map, DatabaseSpec, Dataset } from '@attic/noms';

main().catch(ex => {
  console.error('\nError:', ex);
  if (ex.stack) {
    console.error(ex.stack);
  }
  process.exit(1);
});

let maxItem, lastItem;
let caughtUp = false;
let done = false;

let iter = {};
iter[Symbol.iterator] = function () {
  return {
    next: function () {
      if (caughtUp || done) return { done: true };
      lastItem++;
      if (lastItem == maxItem) caughtUp = true;
      return { value: "/v0/item/" + lastItem, done: false }
    }
  };
};

var all = new Map();

function newItem(v) {
  console.log(v['id']);
  const t = v['type']; // XXX Noms can't deal with a field named 'type'...
  delete v['type'];
  delete v['kids']; // XXX Ignore the array for now.
  const n = newStruct(t, v);

  all.set(v['id'], n).then(a => all = a);
}

async function main(): Promise<void> {
  // Initialize the app with no authentication
  firebase.initializeApp({
    databaseURL: "https://hacker-news.firebaseio.com"
  });

  // The app only has access to public data as defined in the Security Rules
  const fdb = firebase.database();

  var v = await fdb.ref("/v0/maxitem").once("value");

  maxItem = v.val();
  lastItem = 0;

  const process = () => {
    async.eachLimit(iter, 100, (n, done) => {
      const onVal = v => {
        const value = v.val();
        if (value !== null) {
          newItem(value);
          done();
        } else {
          // For unknown reasons we see nulls even for items known to be
          // valid. If we hit this condition, wait a second between retries.
          setTimeout(function () {
            fdb.ref(n).once("value", onVal);
          }, 1000);
        }
      };
      fdb.ref(n).once("value", onVal);
    });
  };

  // Subscribe to the maxitem.
  fdb.ref("/v0/maxitem").on("value", v => {
    maxItem = v.val();
    if (caughtUp) {
      caughtUp = false;
      process();
    }
  });

  process();

  const spec = DatabaseSpec.parse('http://localhost:8000');
  const db = spec.database();
  let ds = new Dataset(db, 'hn');

  let last = null;

  setInterval(() => {
    if (last !== all) {
      ds.commit(all).then(ret => ds = ret).catch(ex => {
        process.exitCode = 1;
        console.log(ex);
        done = true;
      });
      last = all;
    }
  }, 1000 * 1);
}
