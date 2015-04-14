
/*!
 * migrate - Set
 * Copyright (c) 2010 TJ Holowaychuk <tj@vision-media.ca>
 * MIT Licensed
 */

/**
 * Module dependencies.
 */

var EventEmitter = require('events').EventEmitter
  , fs = require('fs')
  , fetchConfig = require('zero-config')
  , mysql = require('mysql')
  , url = require('url');

var NODE_ENV = process.env.NODE_ENV;
var db;
/**
 * Expose `Set`.
 */

module.exports = Set;

/**
 * Initialize a new migration `Set` with the given `path`
 * which is used to store data between migrations.
 *
 * @param {String} path
 * @api private
 */

function Set(path) {
  this.migrations = [];
  this.path = path;
  this.pos = 0;
};

/**
 * Inherit from `EventEmitter.prototype`.
 */

Set.prototype.__proto__ = EventEmitter.prototype;

Set.prototype.loadDbConfig = function(){
  var config = fetchConfig(process.cwd(), {
    dcValue: NODE_ENV === 'production' ? '/etc/uber/datacenter' : null
  });

  return config.get('database');
};

Set.prototype.connectDb = function(fn){
  var self = this,
      config = this.loadDbConfig();

  if(config.master.urlObj) {
    config.master = {
      url: url.format(config.urlObj)
    };
  }

  db = mysql.createConnection(config.master);
  db.connect(function handleConnection(err) {
    if(err) {
      return self.emit('error', err);
    }

    return fn.call(self);
  });
}

Set.prototype.disconnectDb = function(err, result) {
  if(err) {
    return this.emit('error', err);
  }

  this.emit('save');

  db.destroy();
};

/**
 * Save the migration data.
 *
 * @api public
 */

Set.prototype.save = function(){
  var self = this
    , json = JSON.stringify(this);

  var saveToDatabase = function(){
    var query = [
      'INSERT INTO migration',
      'SET',
        'id=1,',
        '?',
      'ON DUPLICATE KEY UPDATE',
        '?',
    ].join(' ');

    var params = {
      migrations: JSON.stringify(this.migrations),
      pos: this.pos
    };

    db.query(query, [params, params], this.disconnectDb.bind(this));
  };

  if(!db) {
    return this.connectDb(saveToDatabase.bind(this));
  }

  saveToDatabase.call(this);
};

/**
 * Load the migration data and call `fn(err, obj)`.
 *
 * @param {Function} fn
 * @return {Type}
 * @api public
 */

Set.prototype.load = function(fn){
  this.emit('load');

  var loadFromDatabase = function() {
    var query = [
      'SELECT',
        '*',
      'FROM migration',
      'WHERE id=1'
    ].join(' ');

    db.query(query, function(err, result) {
      if(err) {
        return fn(err);
      }

      result = result[0] || { migrations: '[]', pos: 0 };

      try {

        result.migrations = JSON.parse(result.migrations);
      } catch (err) {
        return fn(err);
      }

      fn(null, result);
    });
  }

  this.connectDb(loadFromDatabase.bind(this));
};

/**
 * Run down migrations and call `fn(err)`.
 *
 * @param {Function} fn
 * @api public
 */

Set.prototype.down = function(migrationName){
  this.migrate('down', migrationName);
};

/**
 * Run up migrations and call `fn(err)`.
 *
 * @param {Function} fn
 * @api public
 */

Set.prototype.up = function(migrationName){
  this.migrate('up', migrationName);
};

/**
 * Migrate in the given `direction`, calling `fn(err)`.
 *
 * @param {String} direction
 * @param {Function} fn
 * @api public
 */

Set.prototype.migrate = function(direction, migrationName){
  var self = this;
  this.load(function(err, obj){
    if (err) {
      if ('ENOENT' != err.code) return self.emit('error', err);
    } else {
      self.pos = obj.pos;
    }
    self._migrate(direction, migrationName);
  });
};

/**
 * Get index of given migration in list of migrations
 *
 * @api private
 */

 function positionOfMigration(migrations, filename) {
   for(var i=0; i < migrations.length; ++i) {
     if (migrations[i].title == filename) return i;
   }
   return -1;
 }

/**
 * Perform migration.
 *
 * @api private
 */

Set.prototype._migrate = function(direction, migrationName){
  var self = this
    , migrations
    , migrationPos;

  if (!migrationName) {
    migrationPos = direction == 'up' ? this.migrations.length : 0;
  } else if ((migrationPos = positionOfMigration(this.migrations, migrationName)) == -1) {
    console.error("Could not find migration: " + migrationName);
    process.exit(1);
  }

  switch (direction) {
    case 'up':
      migrations = this.migrations.slice(this.pos, migrationPos+1);
      this.pos += migrations.length;
      break;
    case 'down':
      migrations = this.migrations.slice(migrationPos, this.pos).reverse();
      this.pos -= migrations.length;
      break;
  }

  function next(err, migration) {
    // error from previous migration
    if (err) {
      self.emit('error', err);
      return;
    }

    // done
    if (!migration) {
      self.emit('complete');
      self.save();
      return;
    }

    self.emit('migration', migration, direction);
    migration[direction](function(err){
      next(err, migrations.shift());
    });
  }

  next(null, migrations.shift());
};
