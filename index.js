const redis = require("redis");
const uuid = require('uuid/v1');
const _ = require('lodash');
const {promisify} = require('util');

const ObjectID = () => uuid();

class RedisDB {
	constructor(settings) {
		this.settings = _.defaultsDeep({}, {
	        host: "localhost",
	        port: 6379,
	        options: {
	        	password: undefined,
		        socket_keepalive: true,
		        db: 0
	        },
	        prefix: 'db',
	        methods: ['get','set','del','keys','hmset','hgetall','lpush','llen','lindex','lrem','lrange']
	    }, settings);
	    this.db = null;
	    this.client = null;
	}
	connect() {
		return new Promise((resolve, reject) => {
			const client = redis.createClient(Object.assign({
				host: this.settings.host,
				port: this.settings.port
			}, this.settings.options));

		    client.on('connect', () => {
		    	this.db = client;
		    	this.client = this.settings.methods.reduce((a, c) => Object.assign(a, {
		    		[c]: promisify(this.db[c]).bind(this.db)
		    	}), {});
		        resolve(this);
		    });
		});
	}
	bindModel(model, idKey = '_id') {
		let storage = this;

		model.get = function (id) {
			return storage.getByID(model, id);
		};
		model.find = function (filters) {
			return storage.find(model, filters);
		};
		model.clear = async function (filters = {}) {
			let result = await storage.clear(model, filters, true);
			return (result.deletedCount > 0);
		};

		model.prototype.save = async function () {
			try {
				let result = await storage.save(model, Object.assign({}, this));
				if (result[idKey] != undefined) {
					Object.assign(this, result);
				}
				return this;
			} catch (err) {
				throw err;
			}
		};
		model.prototype.remove = async function () {
			let result = await storage.remove(model, this[idKey], true);
			return (result.deletedCount == 1);
		};

		return model;
	}

	combineFilters(filters = []) {
		if (filters['length'] === undefined) filters = [filters];		
		return Object.assign({}, ...filters);
	}

	error(err, message) {
		if (err instanceof Error) return err;
		else return new Error(err || message);
	}
	getModel(model) {
		if (_.isFunction(model)) return {
			name: model.name.toLowerCase(),
			cls: model
		};
		else return model;
	}
	getNewID(name) {
		return ObjectID();
	}
	getKey(model, id = null) {
		if (id) return `${this.settings.prefix}_${model.name}#${id}`;
		else return `${this.settings.prefix}_${model.name}`;
	}
	async save(model, data, idKey = '_id') {
		model = this.getModel(model);

		if (!data[idKey]) {
			//insert
			Object.assign(data, {
				[idKey]: this.getNewID()
			});

			try {
				await this.client.hmset(this.getKey(model, data[idKey]), data);
				await this.client.lpush(this.getKey(model), data[idKey]);

				return { [idKey]: data[idKey] };
			} catch (err) {
				await this.remove(model, data[idKey]);
				throw this.error(err, 'create_failed');
			}
		} else {
			//update
			await this.client.hmset(this.getKey(model, data[idKey]), data);
			return data;
		}
	}
	async remove(model, id, isFinal) {
		if (!id) return {deletedCount:0};

		model = this.getModel(model);
		await this.client.lrem(this.getKey(model), 1, id);
		return {
			deletedCount: await this.client.del(this.getKey(model, id))
		};
	}
	async find(model, filters) {
		model = this.getModel(model);
		let filter = this.combineFilters(filters);
		let key = this.getKey(model);

		const count = await this.client.llen(key);
		const getAll = async () => {
			let ids = await this.client.lrange(key, 0, count);
			let result = [];
			for (let id of ids) result.push(await this.getByID(model, id));
			return result;
		};
		const getPage = async (page, limit = 10) => {
			let from = (page - 1) * limit;
			let to = from + limit;
			let ids = await this.client.lrange(key, from, to);
			let result = [];
			for (let id of ids) result.push(await this.getByID(model, id));
			return result;
		};
		const each = async function* (limit = 10) {
			for (let curPage = 1; curPage <= Math.ceil(count / limit); curPage++) {
				let items = await getPage(curPage, limit);
				for (let item of items) {
					yield item;
				}
			}
		};

		return {
			count,
			getAll,
			getPage,
			each
		};
	}
	async getByID(model, id) {
		model = this.getModel(model);

		try {
			let result = await this.client.hgetall(this.getKey(model, id));
			return new model.cls(result);
		} catch (err) {
			throw this.error(err, 'get_failed');
		}
	}
	async clear(model, filters, isFinal) {
		model = this.getModel(model);
		let query = await this.find(model, filters);
		for await (let item of query.each()) {
			console.log('remove: ', item);
			await item.remove();
		}
		return {deletedCount:query.count};
	}

	static async init(settings = {}, models = []) {
		let db = new RedisDB(settings);
		await db.connect();

		for (let model of models) db.bindModel(model);

		return db;
	}
}

module.exports = {
	RedisDB,
	ObjectID
};