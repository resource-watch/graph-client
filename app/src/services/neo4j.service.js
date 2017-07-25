const logger = require('logger');
const config = require('config');
const neo4j = require('neo4j-driver').v1;

const NEO4J_URI = process.env.NEO4J_URI || `bolt://${config.get('neo4j.host')}:${config.get('neo4j.port')}`;


const CREATE_DATASET = `MERGE (dataset:Dataset {id: {id}}) RETURN dataset`;
const CHECK_EXISTS_DATASET = `MATCH (dataset:Dataset {id: {id}}) RETURN dataset`;
const CHECK_EXISTS_RESOURCE = `MATCH (n:{resourceType} {id: {resourceId}}) RETURN n`;
const CREATE_WIDGET_AND_RELATION = `
  MATCH(dataset: Dataset {id: {idDataset}})
  MERGE (widget: Widget {id: {idWidget}})
  MERGE (widget)-[r:belongsTo]->(dataset) RETURN widget, dataset, r
`;

const CREATE_LAYER_AND_RELATION = `
  MATCH(dataset: Dataset {id: {idDataset}})
  MERGE (layer: Layer {id: {idLayer}})
  MERGE (layer)-[r:belongsTo]->(dataset) RETURN layer, dataset, r
`;

const CREATE_METADATA_AND_RELATION = `
  MATCH (resource:{resourceType} {id: {resourceId}})
  MERGE (metadata: Metadata {id: {idMetadata}})
  MERGE (metadata)-[r:belongsTo]->(resource) RETURN metadata, resource, r
`;

const DELETE_DATASET_NODE = `
  MATCH (n)-[:belongsTo*0..]->(dataset:Dataset{id:{id}})
  DETACH DELETE dataset, n
`;

const DELETE_WIDGET_NODE = `
  MATCH (n)-[:belongsTo*0..]->(widget:Widget{id:{id}}) 
  DETACH DELETE widget, n
`;

const DELETE_LAYER_NODE = `
  MATCH (n)-[:belongsTo*0..]->(layer:Layer{id:{id}}) 
  DETACH DELETE layer, n
`;

const DELETE_METADATA_NODE = `
  MATCH (metadata:Metadata{id:{id}})
  DETACH DELETE metadata
`;

class Neo4JService {

  constructor() {
    logger.info('Connecting to neo4j');
    let driver = null;
    if (config.get('neo4j.password') === null || config.get('neo4j.user') === null) {
      driver = neo4j.driver(NEO4J_URI);
    } else {
      driver = neo4j.driver(NEO4J_URI, neo4j.auth.basic(config.get('neo4j.user'), config.get('neo4j.password')));
    }
    this.session = driver.session();
  }

  async query(query) {
    logger.info('Doing query ONLY READ ', query);

    const readTxResultPromise = this.session.readTransaction((transaction) => {
      return transaction.run(query);
    });
    return readTxResultPromise;
  }

  async checkExistsResource(resourceType, resourceId) {
    logger.debug('Checking if exist resource with type ', resourceType, ' and id ', resourceId);
    return this.session.run(CHECK_EXISTS_RESOURCE.replace('{resourceType}', resourceType), {
      resourceId
    });
  }

  async createDatasetNode(id) {
    logger.debug('Creating dataset with id ', id);
    return this.session.run(CREATE_DATASET, {
      id
    });
  }

  async deleteDatasetNode(id) {
    logger.debug('Deleting dataset with id ', id);
    return this.session.run(DELETE_DATASET_NODE, {
      id
    });
  }

  async checkExistsDataset(id) {
    logger.debug('Checking if exists dataset with id ', id);
    return this.session.run(CHECK_EXISTS_DATASET, {
      id
    });
  }

  async createWidgetNodeAndRelation(idDataset, idWidget) {
    logger.debug('Creating widget and relation with id ', idWidget, '; id dataset', idDataset);
    return this.session.run(CREATE_WIDGET_AND_RELATION, {
      idWidget,
      idDataset
    });
  }

  async deleteWidgetNodeAndRelation(id) {
    logger.debug('Deleting widget and relation with id ', id);
    return this.session.run(DELETE_WIDGET_NODE, {
      id
    });
  }

  async createLayerNodeAndRelation(idDataset, idLayer) {
    logger.debug('Creating layer and relation with id ', idLayer, '; id dataset', idDataset);
    return this.session.run(CREATE_LAYER_AND_RELATION, {
      idLayer,
      idDataset
    });
  }

  async deleteLayerNodeAndRelation(id) {
    logger.debug('Deleting layer and relation with id ', id);
    return this.session.run(DELETE_LAYER_NODE, {
      id
    });
  }

  async createMetadataNodeAndRelation(resourceType, resourceId, idMetadata) {
    logger.debug('Creating metadata and relation with id-metadata ', idMetadata, '; resourceType ', resourceType, 'id dataset', resourceId);
    return this.session.run(CREATE_METADATA_AND_RELATION.replace('{resourceType}', resourceType), {
      idMetadata,
      resourceId
    });
  }

  async deleteMetadata(id) {
    logger.debug('Deleting metadata and relation with id ', id);
    return this.session.run(DELETE_METADATA_NODE, {
      id
    });
  }

}

module.exports = new Neo4JService();
