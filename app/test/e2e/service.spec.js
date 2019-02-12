const logger = require('logger');
const nock = require('nock');
const BASE_URL = require('./test.constants').BASE_URL;
require('should');

describe('E2E test', () => {

    before(() => {

        // simulating gateway communications
        nock(`${process.env.CT_URL}/v1`)
            .post('/', () => true)
            .reply(200, {
                status: 200,
                detail: 'Ok'
            });
    });

    after(() => {
    });
});
