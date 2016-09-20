const fetch = require('node-fetch');
const co = require('co');
const fs = require('fs');
const Converter = require('csvtojson').Converter;

const tagFacetsHost = 'http://tag-facets-api.ft.com';

const csvConverter = new Converter({});
csvConverter.on('end_parsed', (userObj) => {
  co(function* mapFollows() {
    let processedCount = 0;
    let missingAuthorFollows = '';
    let unmappedFollows = '';

    for (const user of userObj) {
      const authorName = user.taxonomyName.toLowerCase();
      try {
        if (!user.taxonomyName) {
          unmappedFollows += `${user.userId}\n`;
        } else {
          const tagRes = yield fetch(`${tagFacetsHost}?taxonomies=authors&partial=${authorName}`);
          const tag = yield tagRes.json();
          if (!tag.length) {
            missingAuthorFollows += `${user.userId},${user.taxonomyName}\n`;
          }
        }
      } catch (err) {
        console.log(`Error fetching tag for ${user.userId}`);
      }
      console.log(processedCount += 1);
    }

    fs.writeFileSync('missingauthors.csv', missingAuthorFollows, 'utf8');
    fs.writeFileSync('nomapping.csv', unmappedFollows, 'utf8');
  });
});

fs.createReadStream('./author_alerts.csv').pipe(csvConverter);
