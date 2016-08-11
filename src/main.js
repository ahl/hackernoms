// @flow

import firebase from 'firebase';
import async from 'async';
import { newStruct, Map, List, DatasetSpec, Dataset } from '@attic/noms';
import Pace from 'pace';

let maxItem, lastItem;
let caughtUp = false;
let done = false;
let extra = [];

let iter = {};
iter[Symbol.iterator] = function () {
  return {
    next: function () {
      if (caughtUp || done)
        return { done: true };

      var next = extra.pop();
      if (next)
        return { value: "/v0/item/" + next, done: false }

      lastItem++;
      if (lastItem == maxItem) caughtUp = true;
      return { value: "/v0/item/" + lastItem, done: false }
    }
  };
};

var all = Promise.resolve(new Map());
var changed = false;

function newItem(v) {
  const t = v['type']; // XXX Noms can't deal with a field named 'type'...
  delete v['type'];
  v['kids'] = new List(v['kids']);
  const item = newStruct(t, v);

  changed = true;
  all = all.then(a => {
    return a.set(v['id'], item);
  });
}

let pg;

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
  //lastItem = maxItem - 10;

  pg = new Pace({
    'total': maxItem + 1
  });

  pg.op(lastItem);

  const process = () => {
    async.eachLimit(iter, 100, (n, done) => {
      //console.log(n);
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
    pg.total = maxItem + 1;
    if (caughtUp) {
      caughtUp = false;
      process();
    }
  });

  // Subscribe to updates.
  fdb.ref("v0/updates").on('value', v => {
    //console.log(v.val()['items']);
    v.val()['items'].forEach(n => {
      if (n <= lastItem)
        extra.push(n);
    });
    if (caughtUp) {
      caughtUp = false;
      process();
    }
  });

  process();

  const spec = DatasetSpec.parse('http://localhost:8000::hn');
  let ds = spec.dataset();

  const delay = 1 * 1000;

  const commit = () => {
    all.then(a => {

      // Nothing changed; better luck next time.
      if (!changed) {
        setTimeout(commit, delay);
        return;
      }

      const count = a.size;

      pg.op(count);
      changed = false;
      all = ds.commit(a).then(nds => {
        ds = nds;

        setTimeout(commit, 0);

        return ds.headValue();
      }).catch(ex => {
        process.exitCode = 1;
        console.log(ex);
        done = true;
      });
    });
  };

  commit();
}

main().catch(ex => {
  console.error('\nError:', ex);
  if (ex.stack) {
    console.error(ex.stack);
  }
  process.exit(1);
});

