const fastify = require('fastify')({logger:true,caseSensitive: false,bodyLimit: 50 * 1024 * 1024,});

fastify.register(require('@fastify/cors'),{
    origin:["*","http://localhost:3001","https://peopletrust.io",'https://buy.peopletrust.io']
})
fastify.register(require('@fastify/formbody'));

fastify.register(require('./Plugins/Mysql'))

fastify.register(require('./Controllers/UserController'),{prefix:"/api/v2"});
fastify.register(require('./Controllers/AdminController'),{prefix:"/api/admin"})

fastify.listen({ port: 3005 }, (err, address) => {
    if (err) {
      fastify.log.error(err);
      process.exit(1);
    }
    fastify.log.info(`Server listening at ${address}`);
  });