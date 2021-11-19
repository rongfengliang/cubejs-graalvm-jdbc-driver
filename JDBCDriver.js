/* eslint-disable no-restricted-syntax,import/no-extraneous-dependencies */
const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
const SqlString = require('sqlstring');
const { promisify } = require('util');
const genericPool = require('generic-pool');
// use graalvm js invoke
const DriverManager = Java.type('java.sql.DriverManager')
const SupportedDrivers = require('./supported-drivers');

const initClassPath = (customClassPath) => {
  // use graalvm method
  Java.addToClasspath(customClassPath)
};

const applyParams = (query, params) => SqlString.format(query, params);

module.exports = class JDBCDriver extends BaseDriver {
  /**
   * @param {Partial<JDBCDriverConfiguration>} [config]
   */
  constructor(config = {}) {
    super();

    const { poolOptions, ...dbOptions } = config || {};

    const dbTypeDescription = JDBCDriver.dbTypeDescription(config.dbType || process.env.CUBEJS_DB_TYPE);

    /** @protected */
    this.config = {
      dbType: process.env.CUBEJS_DB_TYPE,
      url: process.env.CUBEJS_JDBC_URL || dbTypeDescription && dbTypeDescription.jdbcUrl(),
      drivername: process.env.CUBEJS_JDBC_DRIVER || dbTypeDescription && dbTypeDescription.driverClass,
      properties: dbTypeDescription && dbTypeDescription.properties,
      ...dbOptions
    };

    if (!this.config.drivername) {
      throw new Error('drivername is required property');
    }

    if (!this.config.url) {
      throw new Error('url is required property');
    }

    /** @protected */
    this.pool = genericPool.createPool({
      create: async () => {
        initClassPath(this.getCustomClassPath())
        if (!this.jdbcProps) {
          /** @protected */
          this.jdbcProps = this.getJdbcProperties();
        }
        return DriverManager.getConnection(this.config.url, this.jdbcProps)
      },
      destroy: async (connection) => {connection.close()},
      validate: (connection) => {
        try {
          return connection.isValid(this.testConnectionTimeout() / 1000);
        } catch (e) {
          return false;
        }
      }
    }, {
      min: 0,
      max: process.env.CUBEJS_DB_MAX_POOL && parseInt(process.env.CUBEJS_DB_MAX_POOL, 10) || 8,
      evictionRunIntervalMillis: 10000,
      softIdleTimeoutMillis: 30000,
      idleTimeoutMillis: 30000,
      testOnBorrow: true,
      acquireTimeoutMillis: 20000,
      ...poolOptions
    });
  }

  /**
   * @protected
   * @return {Promise<string|undefined>}
   */
  async getCustomClassPath() {
    return this.config.customClassPath;
  }

  /**
   * @protected
   */
  getJdbcProperties() {
    const Properties = Java.type('java.util.Properties');
    const properties = new Properties();
    for (const [name, value] of Object.entries(this.config.properties)) {
      properties.put(name, value);
    }

    return properties;
  }

  /**
   * @public
   * @return {Promise<*>}
   */
  testConnection() {
    return this.query('SELECT 1', []);
  }

  /**
   * @protected
   */
  prepareConnectionQueries() {
    const dbTypeDescription = JDBCDriver.dbTypeDescription(this.config.dbType);
    return this.config.prepareConnectionQueries ||
      dbTypeDescription && dbTypeDescription.prepareConnectionQueries ||
      [];
  }

  /**
   * @public
   * @return {Promise<any>}
   */
  async query(query, values) {
    const queryWithParams = applyParams(query, values);
    const cancelObj = {};
    const promise = this.queryPromised(queryWithParams, cancelObj, this.prepareConnectionQueries());
    promise.cancel =
      () => cancelObj.cancel && cancelObj.cancel() || Promise.reject(new Error('Statement is not ready'));
    return promise;
  }

  /**
   * @protected
   */
  async withConnection(fn) {
    const conn = await this.pool.acquire();

    try {
      return await fn(conn);
    } finally {
      await this.pool.release(conn);
    }
  }

  /**
   * @protected
   */
  async queryPromised(query, cancelObj, options) {
    options = options || {};
    try {
      const conn = await this.pool.acquire();
      try {
        const prepareConnectionQueries = options.prepareConnectionQueries || [];
        for (let i = 0; i < prepareConnectionQueries.length; i++) {
          await this.executeStatement(conn, prepareConnectionQueries[i]);
        }
        return await this.executeStatement(conn, query, cancelObj);
      } finally {
        await this.pool.release(conn);
      }
    } catch (ex) {
      if (ex.cause) {
        throw new Error(ex.cause.getMessage());
      } else {
        throw ex;
      }
    }
  }

  /**
   * @protected
   */
  async executeStatement(conn, query, cancelObj) {
    const statement =  conn.createStatement();
    if (cancelObj) {
      cancelObj.cancel = promisify(statement.cancel.bind(statement));
    }
    statement.setQueryTimeout(600);
    const resultSet =  statement.executeQuery(query);
    const toObjArrayAsync =
      resultSet.toObjArray && promisify(resultSet.toObjArray.bind(resultSet)) ||
      (() => Promise.resolve(resultSet));

    return toObjArrayAsync();
  }

  /**
   * @public
   * @return {Promise<void>}
   */
  async release() {
    await this.pool.drain();
    await this.pool.clear();
  }

  /**
   * @public
   * @return {string[]}
   */
  static getSupportedDrivers() {
    return Object.keys(SupportedDrivers);
  }

  /**
   * @public
   * @param {string} dbType
   * @return {Object}
   */
  static dbTypeDescription(dbType) {
    return SupportedDrivers[dbType];
  }
}
