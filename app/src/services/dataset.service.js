const logger = require('logger');
const ctRegisterMicroservice = require('ct-register-microservice-node');

class DatasetService {

  static async checkDatasets(datasets, query) {
    logger.info('Checking published and other fields of dataset', datasets);
    let queryParams = '';
    let url = '/dataset/find-by-ids';
    if (query.published) {
      if (queryParams) {
        queryParams += '&';
      }
      queryParams += `published=${query.published}`;
    }

    if (query.env) {
      if (queryParams) {
        queryParams += '&';
      }
      queryParams += `env=${query.env}`;
    }
    if (queryParams !== '') {
      url += `?${queryParams}`;
    }
    
    const result = await ctRegisterMicroservice.requestToMicroservice({
      uri: url,
      method: 'POST',
      json: true,
      body: {
        ids: datasets
      }
    });
    logger.debug('Returning ', result);
    if (result && result.data) {
      return result.data.map(el => el.id);
    }
    return [];
    
  }

}

module.exports = DatasetService;
