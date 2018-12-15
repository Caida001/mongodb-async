const mongodb = require('mongodb')
const async = require('async')

const customers = require('./m3-customer-data.json')
const addresses = require('./m3-customer-address-data.json')



let tasks = []
const batchjobs = parseInt(process.argv[2]) || 1000
let start = 0
let end = 0

const url = "mongodb://localhost:27017/ed-course-db"

mongodb.MongoClient.connect(url, (err, db) => {

  if(err) return process.exit(1);

  customers.forEach((customer, index) => {
    customers[index] = Object.assign(customer, addresses[index])
    start = index
    end = (start + batchjobs > customers.length) ? customers.length-1 : start + batchjobs


    tasks.push((done) => {
      console.log(`Processing ${start}-${end} out of ${customers.length}`);
      db.collection('customers').insert(customers.slice(start, end), (error, results) => {
        done(error, results)
      })
    })

  })


  async.parallel(tasks, (err, results) => {
    if(err){
      console.log(err)
    } else {
      console.log("processed ")
    }
    db.close();
  })

})
