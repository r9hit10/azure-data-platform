const crm = require("../shared/crm");
module.exports = async function (context, req) {
  crm.respond(context, () => crm.meta(req.query || {}));
};
