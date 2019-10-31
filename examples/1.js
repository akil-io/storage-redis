const { RedisDB } = require("../index.js");

class Profile {
	constructor(fields) {
		this._id = null;
		this.title = "";
		this.email = "";
		this.password = "";

		Object.assign(this, fields);
	}
}

(async () => {
	let db = await RedisDB.init({}, [Profile]);

	await Profile.clear();

	let p = new Profile({
		title: "Alex",
		email: "test@test.ru",
		password: "testingpas"
	});

	await p.save();
	console.log('Saved with ID: ', p._id);

	let list = await (await Profile.find({})).getAll();
	console.log(list);

	let p2 = await Profile.get(p._id);
	console.log("saved profile: ", p2);

	console.log("deleted: ", await p2.remove());
	console.log("Stored in DB: ", (await Profile.find({})).count);

})().then(() => {
	console.log('Complete.');
	process.exit();
}).catch(err => {
	console.error("Error: ", err);
	process.exit();
})