const nock = require('nock');
const chai = require('chai');
const { getTestServer } = require('./utils/test-server');

chai.should();

let requester;

nock.disableNetConnect();
nock.enableNetConnect(process.env.HOST_IP);

describe('GET list concepts', () => {
    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        requester = await getTestServer();
    });

    it('Getting a list of concepts should return a 200 status code (happy case)', async () => {
        const response = await requester
            .get('/api/v1/graph/query/list-concepts');

        response.status.should.equal(200);
    });

    afterEach(() => {
        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });
});
