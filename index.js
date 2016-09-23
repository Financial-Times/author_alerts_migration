require('dotenv').config({ silent: true });

const fetch = require('node-fetch');
const co = require('co');
const fs = require('fs');
const Converter = require('csvtojson').Converter;
const config = require('./config');

const missingTags = require('./missingtags.json');

const tagFacetsHost = 'http://tag-facets-api.ft.com';

const csvConverter = new Converter({});
csvConverter.on('end_parsed', (userObj) => {
  co(function* mapFollows() {
    let processedCount = 0;
    let missingCount = 0;

    const options = {
      method: 'post',
      headers: {
        'Content-type': 'application/json',
        'X-API-KEY': config.myFtAuth
      }
    };

    for (const user of userObj) {
      const url = `${config.myFtHost}/v3/user/${user.userId}/followed/concept`;
      const authorName = user.taxonomyName.toLowerCase();
      try {
        if (user.taxonomyName) {
          const tagRes = yield fetch(`${tagFacetsHost}?taxonomies=authors&partial=${authorName}`);
          const tag = yield tagRes.json();
          if (tag.length) {
            options.body = JSON.stringify({
              uuid: tag[0].id,
              name: tag[0].name,
              taxonomy: tag[0].taxonomy,
              _rel: {
                instant: user.immediate
              }
            });

            const myFtRes = yield fetch(url, options);
            console.log(myFtRes.status);
          } else {
            const mapping = missingTags.find(missingTag => missingTag.Author.toLowerCase() === authorName);
            if (mapping) {
              options.body = JSON.stringify({
                uuid: mapping.ConceptId,
                name: mapping.Author,
                taxonomy: mapping.Taxonomy,
                _rel: {
                  instant: user.immediate
                }
              });

              const myFtRes = yield fetch(url, options);
              console.log(myFtRes.status);
            }
          }
        }
      } catch (err) {
        console.log(err);
        missingCount++;
        console.log(`Error for ${user.userId}`);
      }
      console.log(processedCount += 1);
    }
    console.log('missing:', missingCount);
  });
});

fs.createReadStream('./author_alerts.csv').pipe(csvConverter);
