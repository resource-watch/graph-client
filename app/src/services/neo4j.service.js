const logger = require('logger');
const config = require('config');
const neo4j = require('neo4j-driver').v1;

const NEO4J_URI = process.env.NEO4J_URI || `bolt://${config.get('neo4j.host')}:${config.get('neo4j.port')}`;


const CREATE_DATASET = `MERGE (dataset:DATASET {id: {id}}) RETURN dataset`;
const CHECK_EXISTS_DATASET = `MATCH (dataset:DATASET {id: {id}}) RETURN dataset`;
const CHECK_EXISTS_RESOURCE = `MATCH (n:{resourceType} {id: {resourceId}}) RETURN n`;
const CREATE_USER = `MERGE (user:USER {id: {id}}) RETURN user`;
const CHECK_EXISTS_USER = `MATCH (dataset:USER {id: {id}}) RETURN dataset`;

const CREATE_RELATION = `
  MATCH (resource:{resourceType} {id:{resourceId}})
  MERGE (concept:CONCEPT{id:{label}})
  MERGE (resource)-[r:TAGGED_WITH]->(concept) RETURN concept, resource, r
`;

const CREATE_RELATION_FAVOURITE_AND_RESOURCE = `
MATCH (resource:{resourceType} {id:{resourceId}})
MERGE (user:USER{id:{userId}})
MERGE (user)-[r:FAVOURITE]->(concept) RETURN user, resource, r
`;

const CREATE_WIDGET_AND_RELATION = `
  MATCH(dataset: DATASET {id: {idDataset}})
  MERGE (widget: WIDGET {id: {idWidget}})
  MERGE (widget)-[r:BELONGS_TO]->(dataset) RETURN widget, dataset, r
`;

const CREATE_LAYER_AND_RELATION = `
  MATCH(dataset: DATASET {id: {idDataset}})
  MERGE (layer: LAYER {id: {idLayer}})
  MERGE (layer)-[r:BELONGS_TO]->(dataset) RETURN layer, dataset, r
`;

const CREATE_METADATA_AND_RELATION = `
  MATCH (resource:{resourceType} {id: {resourceId}})
  MERGE (metadata: METADATA {id: {idMetadata}})
  MERGE (metadata)-[r:BELONGS_TO]->(resource) RETURN metadata, resource, r
`;

const DELETE_DATASET_NODE = `
  MATCH (n)-[:BELONGS_TO*0..]->(dataset:DATASET{id:{id}})
  DETACH DELETE dataset, n
`;


const DELETE_RELATION_FAVOURITE_AND_RESOURCE = `
  MATCH (user:USER{id:{userId}})-[r:FAVOURITE]->(resource:{resourceType} {id:{resourceId}})
  DETACH DELETE r
`;


const DELETE_WIDGET_NODE = `
  MATCH (n)-[:BELONGS_TO*0..]->(widget:WIDGET{id:{id}}) 
  DETACH DELETE widget, n
`;

const DELETE_LAYER_NODE = `
  MATCH (n)-[:BELONGS_TO*0..]->(layer:LAYER{id:{id}}) 
  DETACH DELETE layer, n
`;

const DELETE_METADATA_NODE = `
  MATCH (metadata:METADATA{id:{id}})
  DETACH DELETE metadata
`;

const QUERY_SIMILAR_DATASET = `
MATCH p=(d:DATASET{id:{dataset}})-[:TAGGED_WITH]->(c:CONCEPT)<-[:TAGGED_WITH]-(d2:DATASET)
WITH count(p) AS number_of_shared_concepts, COLLECT(c.id) AS shared_concepts, d2
RETURN d2.id, shared_concepts, number_of_shared_concepts
ORDER BY number_of_shared_concepts DESC
`;

const QUERY_SIMILAR_DATASET_WITH_DESCENDENT = `
MATCH (d:DATASET{id:{dataset}})-[:TAGGED_WITH]->(c:CONCEPT)
WITH COLLECT(c.id) AS main_tags
MATCH (d2:DATASET)-[:TAGGED_WITH]->(c1:CONCEPT)-[*]->(c2:CONCEPT)
WHERE c1.id IN main_tags OR c2.id IN main_tags
WITH COLLECT(DISTINCT c1.id) AS dataset_tags, d2.id AS dataset
WITH size(dataset_tags) AS number_of_ocurrences, dataset_tags, dataset
RETURN dataset, dataset_tags, number_of_ocurrences
ORDER BY number_of_ocurrences DESC
`;

const QUERY_SEARCH_PARTS= [`
MATCH (c:CONCEPT)<-[*]-(c2:CONCEPT)<-[:TAGGED_WITH]-(d:DATASET)
WHERE (c.id IN ['africa'] OR c2.id IN ['africa'])
`, `
WITH COLLECT(d.id) AS datasets
MATCH (c:CONCEPT)<-[*]-(c2:CONCEPT)<-[:TAGGED_WITH]-(d:DATASET)
WHERE (c.id IN {concepts2} OR c2.id IN {concepts2}) AND d.id IN datasets
`, `
WITH COLLECT(d.id) AS intersection
MATCH (c:CONCEPT)<-[*]-(c2:CONCEPT)<-[:TAGGED_WITH]-(d:DATASET)
WHERE (c.id IN {concepts3} OR c2.id IN {concepts3}) AND d.id IN intersection
`];

const QUERY_SEARCH_FINAL = `
RETURN DISTINCT d.id
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

  async createRelationWithConcepts(resourceType, resourceId, concepts) {
    logger.debug('Creating relations with concepts, Type ', resourceType, ' and id ', resourceId, 'and concepts', concepts);
    for (let i = 0, length = concepts.length; i < length; i++) {
      logger.debug(CREATE_RELATION.replace('{resourceType}', resourceType));
      await this.session.run(CREATE_RELATION.replace('{resourceType}', resourceType), {
        resourceId,
        label: concepts[i]
      });
    }
  }

  async createFavouriteRelationWithResource(userId, resourceType, resourceId) {
    
    logger.debug('Creating favourite relation, Type ', resourceType, ' and id ', resourceId, 'and user', userId);
    logger.debug('Checking if exist user');
    const users = await this.session.run(CHECK_EXISTS_USER, {
      id: userId
    });
    if (!users.records || users.records.length === 0) {
      logger.debug('Creating user node');
      await this.session.run(CREATE_USER, {
        id: userId
      });
    }
    await this.session.run(CREATE_RELATION_FAVOURITE_AND_RESOURCE.replace('{resourceType}', resourceType), {
      resourceId,
      userId
    });
  }

  async deleteFavouriteRelationWithResource(userId, resourceType, resourceId) {
    
    logger.debug('deleting favourite relation, Type ', resourceType, ' and id ', resourceId, 'and user', userId);
    
    await this.session.run(DELETE_RELATION_FAVOURITE_AND_RESOURCE.replace('{resourceType}', resourceType), {
      resourceId,
      userId
    });
  }

  async createDatasetNode(id) {
    logger.debug('Creating dataset with id ', id);
    return this.session.run(CREATE_DATASET, {
      id
    });
  }

  async createUserNode(id) {
    logger.debug('Creating user with id ', id);
    return this.session.run(CREATE_USER, {
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

  async createUserNodeAndRelation(idDataset, idWidget) {
    logger.debug('Creating user and relation with id ', idWidget, '; id dataset', idDataset);
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

  async querySimilarDatasets(dataset) {
    logger.debug('Obtaining similar datasets of ', dataset);
    return this.session.run(QUERY_SIMILAR_DATASET, {
      dataset
    });
  }

  async querySimilarDatasetsWithDescendent(dataset) {
    logger.debug('Obtaining similar datasets with descendent of ', dataset);
    return this.session.run(QUERY_SIMILAR_DATASET_WITH_DESCENDENT, {
      dataset
    });
  }

  async querySearchDatasets(concepts) {
    logger.debug('Searching datasets with concepts ', concepts);
    let query = '';
    const params = {
      concepts1: [],
      concepts2: [],
      concepts3: []
    };
    if (concepts && concepts.length > 0) {
      for (let i = 0, length = concepts.length; i < length; i++) {
        query += QUERY_SEARCH_PARTS[i];
        params[`concepts${i}`] = concepts[i];
      }
      query += QUERY_SEARCH_FINAL;
    }
    if (query) {
      return this.session.run(query, params);
    } 
    return null;
  }

}

module.exports = new Neo4JService();
