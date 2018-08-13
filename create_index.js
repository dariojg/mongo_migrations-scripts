var db = db.getSiblingDB('customers')

db.customers.createIndex({ "fiscalId": 1 }, {name: "fiscalId" })
db.customers.createIndex({ "email": 1}, {name: "email" })