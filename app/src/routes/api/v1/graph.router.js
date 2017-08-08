const logger = require('logger');
const Router = require('koa-router');
const neo4jService = require('services/neo4j.service');


const router = new Router({
  prefix: '/graph',
});

class GraphRouter {

  static async query(ctx) {
    ctx.assert(ctx.query.query, 400, '\'query\' query param required');
    ctx.body = await neo4jService.query(ctx.query.query);
  }

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

router.get('/query', GraphRouter.query);
router.post('/dataset/:id', isAuthorized, GraphRouter.createDataset);
router.post('/widget/:idDataset/:idWidget', isAuthorized, checkExistsDataset, GraphRouter.createWidgetNodeAndRelation);
router.post('/layer/:idDataset/:idLayer', isAuthorized, checkExistsDataset, GraphRouter.createLayerNodeAndRelation);
router.post('/metadata/:resourceType/:idResource/:idMetadata', isAuthorized, checkExistsResource, GraphRouter.createMetadataNodeAndRelation);
router.post('/:resourceType/:idResource/associate', isAuthorized, checkExistsResource, GraphRouter.associateResource);

router.delete('/dataset/:id', isAuthorized, GraphRouter.deleteDataset);
router.delete('/widget/:id', isAuthorized, GraphRouter.deleteWidgetNodeAndRelation);
router.delete('/layer/:id', isAuthorized, GraphRouter.deleteLayerNodeAndRelation);
router.delete('/metadata/:id', isAuthorized, GraphRouter.deleteMetadata);

module.exports = router;
