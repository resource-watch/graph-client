const logger = require('logger');
const Router = require('koa-router');
const neo4jService = require('services/neo4j.service');
const qs = require('qs');


const router = new Router({
  prefix: '/graph',
});

class GraphRouter {

  static async createDataset(ctx) {
    logger.info('Creating dataset node with id ', ctx.params.id);
    ctx.body = await neo4jService.createDatasetNode(ctx.params.id);
  }

  static async associateResource(ctx) {
    ctx.assert(ctx.request.body && ctx.request.body.tags && ctx.request.body.tags.length > 0, 400, 'Tags body param is required');
    logger.info('Associating ', ctx.params.resourceType, ' node with id ', ctx.params.idResource, 'and tags ', ctx.request.body.tags);
    ctx.body = await neo4jService.createRelationWithConcepts(ctx.params.resourceType, ctx.params.idResource, ctx.request.body.tags);
  }

  static async createWidgetNodeAndRelation(ctx) {
    logger.info('Creating widget node and relation with idWidget ', ctx.params.idWidget, ' and dataset ', ctx.params.idDataset);
    ctx.body = await neo4jService.createWidgetNodeAndRelation(ctx.params.idDataset, ctx.params.idWidget);
  }

  static async createLayerNodeAndRelation(ctx) {
    logger.info('Creating layer node and relation with idLayer ', ctx.params.idLayer, ' and dataset ', ctx.params.idDataset);
    ctx.body = await neo4jService.createLayerNodeAndRelation(ctx.params.idDataset, ctx.params.idLayer);
  }

  static async createMetadataNodeAndRelation(ctx) {
    ctx.assert(['DATASET', 'LAYER', 'WIDGET'].indexOf(ctx.params.resourceType) >= 0, 400, `Resource ${ctx.params.resourceType} invalid`);
    logger.info('Creating metadata node and relation with idWidget ', ctx.params.idMetadata, ' and resourcetype ', ctx.params.resourceType, ' and idresource', ctx.params.idResource);
    ctx.body = await neo4jService.createMetadataNodeAndRelation(ctx.params.resourceType, ctx.params.idResource, ctx.params.idMetadata);
  }


  static async deleteDataset(ctx) {
    logger.info('Deleting dataset node with id ', ctx.params.id);
    ctx.body = await neo4jService.deleteDatasetNode(ctx.params.id);
  }

  static async deleteWidgetNodeAndRelation(ctx) {
    logger.info('Deleting widget node and relation with idWidget ', ctx.params.id);
    ctx.body = await neo4jService.deleteWidgetNodeAndRelation(ctx.params.id);
  }

  static async deleteLayerNodeAndRelation(ctx) {
    logger.info('Deleting layer node and relation with idWidget ', ctx.params.id);
    ctx.body = await neo4jService.deleteLayerNodeAndRelation(ctx.params.id);
  }

  static async deleteMetadata(ctx) {
    logger.info('Deleting metadata node and relation with idWidget ', ctx.params.id);
    ctx.body = await neo4jService.deleteMetadata(ctx.params.id);
  }

  static async createFavouriteRelationWithResource(ctx) {
    logger.info('Creating favourite relation ');
    ctx.body = await neo4jService.createFavouriteRelationWithResource(ctx.params.userId, ctx.params.resourceType, ctx.params.idResource);
  }

  static async deleteFavouriteRelationWithResource(ctx) {
    logger.info('Creating favourite relation ');
    ctx.body = await neo4jService.deleteFavouriteRelationWithResource(ctx.params.userId, ctx.params.resourceType, ctx.params.idResource);
  }

  static async conceptsInferred(ctx) {
    let concepts = null;
    if (ctx.query.concepts) {
      concepts = ctx.query.concepts.split(',').map(c => c.trim());
    } else if (ctx.request.body) {
      concepts = ctx.request.body.concepts;
    }
    ctx.assert(concepts, 'Concepts is required');
    logger.info('Getting concepts inferred ', concepts);
    ctx.body = await neo4jService.conceptsInferred(concepts);
  }

  static async listConcepts(ctx) {
    logger.info('Getting list concepts ');
    ctx.body = await neo4jService.listConcepts();
  }

  static async querySearchDatasets(ctx) {
    let concepts = null;
    if (ctx.method === 'GET') {
      ctx.assert(ctx.query.concepts, 400, 'Concepts query params is required');
      concepts = ctx.query.concepts;
    } else {
      concepts = ctx.request.body.concepts;
    }
    logger.info('Searching dataset with concepts', concepts);
    const results = await neo4jService.querySearchDatasets(concepts);
    ctx.body = {
      data: results.records ? results.records.map(el => el._fields[0]) : []
    };
  }

  static async querySimilarDatasets(ctx) {
    logger.info('Obtaining similar datasets', ctx.params.dataset);
    const results = await neo4jService.querySimilarDatasets(ctx.params.dataset);
    ctx.body = {
      data: results && results.records ? results.records.slice(0, ctx.query.limit || 3).map((el) => {
        return {
          dataset: el._fields[0],
          concepts: el._fields[1],
          numberOfOcurrences: el._fieldLookup.shared_concepts
        };
      }) : []
    };
  }

  static async querySimilarDatasetsIncludingDescendent(ctx) {
    logger.info('Obtaining similar datasets with descendent', ctx.params.dataset);
    const results = await neo4jService.querySimilarDatasetsIncludingDescendent(ctx.params.dataset);
    ctx.body = {
      data: results && results.records ? results.records.slice(0, ctx.query.limit || 3).map((el) => {
        return {
          dataset: el._fields[0],
          concepts: el._fields[1],
          numberOfOcurrences: el._fieldLookup.dataset_tags
        };
      }) : []
    };
  }

}

async function isAuthorized(ctx, next) {
  let user = ctx.request.body && ctx.request.body.loggedUser;
  if (!user && ctx.request.query && ctx.request.query.loggedUser) {
    user = JSON.parse(ctx.request.query.loggedUser);
  }

  if (!user || user.id !== 'microservice') {
    ctx.throw(403, 'Not authorized');
    return;
  }
  await next();
}


async function checkExistsDataset(ctx, next) {
  const exists = await neo4jService.checkExistsDataset(ctx.params.idDataset);
  if (!exists.records || exists.records.length === 0) {
    ctx.throw(404, 'Dataset not found');
    return;
  }
  await next();
}

async function checkExistsResource(ctx, next) {
  ctx.params.resourceType = ctx.params.resourceType.toUpperCase();
  const exists = await neo4jService.checkExistsResource(ctx.params.resourceType, ctx.params.idResource);
  if (!exists.records || exists.records.length === 0) {
    ctx.throw(404, `Resource ${ctx.params.resourceType} and id ${ctx.params.idResource} not found`);
    return;
  }
  logger.debug('Exists');
  await next();
}

router.get('/query/list-concepts', GraphRouter.listConcepts);
router.get('/query/concepts-inferred', GraphRouter.conceptsInferred);
router.post('/query/concepts-inferred', GraphRouter.conceptsInferred);
router.get('/query/similar-dataset/:dataset', GraphRouter.querySimilarDatasets);
router.get('/query/similar-dataset-including-descendent/:dataset', GraphRouter.querySimilarDatasetsIncludingDescendent);
router.get('/query/search-datasets', GraphRouter.querySearchDatasets);
router.post('/query/search-datasets', GraphRouter.querySearchDatasets);


router.post('/dataset/:id', isAuthorized, GraphRouter.createDataset);
router.post('/widget/:idDataset/:idWidget', isAuthorized, checkExistsDataset, GraphRouter.createWidgetNodeAndRelation);
router.post('/layer/:idDataset/:idLayer', isAuthorized, checkExistsDataset, GraphRouter.createLayerNodeAndRelation);
router.post('/metadata/:resourceType/:idResource/:idMetadata', isAuthorized, checkExistsResource, GraphRouter.createMetadataNodeAndRelation);
router.post('/:resourceType/:idResource/associate', isAuthorized, checkExistsResource, GraphRouter.associateResource);
router.post('/favourite/:resourceType/:idResource/:userId', isAuthorized, checkExistsResource, GraphRouter.createFavouriteRelationWithResource);

router.delete('/favourite/:resourceType/:idResource/:userId', isAuthorized, checkExistsResource, GraphRouter.deleteFavouriteRelationWithResource);
router.delete('/dataset/:id', isAuthorized, GraphRouter.deleteDataset);
router.delete('/widget/:id', isAuthorized, GraphRouter.deleteWidgetNodeAndRelation);
router.delete('/layer/:id', isAuthorized, GraphRouter.deleteLayerNodeAndRelation);
router.delete('/metadata/:id', isAuthorized, GraphRouter.deleteMetadata);

module.exports = router;
