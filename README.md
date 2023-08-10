

![image](https://github.com/ypenn21/timeseries/assets/6451406/8033c59b-664e-4972-b79b-256e2c3e6a11)

Architecture in slides: https://docs.google.com/presentation/d/13rTYjMldWWJLFahSa9YHZlR1NLbRvBQHr4dhZyIWBjQ/edit#slide=id.g1e547da9be2_0_451


Adding an intermediate service, like a transformer, between the publisher-subscriber (pub-sub) and a database writer (db-writer) in an event-driven architecture requires a series of changes to the existing workflow. Here's a step-by-step guide on how to make those changes:

Define the Transformer Service: The transformer will need to consume events from the pub-sub system, transform the data as required, and then pass it on to the db-writer. You will need to design the transformation logic as per your requirement.

Update the Pub-Sub Topic/Queue: If the db-writer is subscribed to a particular topic or queue in the pub-sub system, you will need to update this subscription to point to the transformer instead. The transformer will then have to subscribe to the same topic or queue that the db-writer was previously subscribed to.

Create a New Topic/Queue (Optional): Depending on the pub-sub system you are using, you may want to create a new topic or queue specifically for communication between the transformer and db-writer. This can isolate the transformation flow and make it easier to manage.

Modify the DB-Writer: The db-writer should now listen to the events from the transformer instead of directly from the pub-sub system. This may require changing the subscription endpoint or connection details in the db-writer code.

Implement Error Handling and Monitoring: Adding a new service into the flow will require additional error handling and monitoring to ensure that failures in the transformer don't result in lost data or other issues.

Testing: Thoroughly test the new workflow with the transformer service in place. It would be best to do this in a non-production environment to ensure that the transformation logic is correct and that the overall flow of events through the system is as expected.

Deployment: Once testing is complete, you can deploy the transformer service and the modified db-writer into your production environment.

Scaling Considerations: Depending on the volume of events and the complexity of the transformation logic, you may need to consider scaling the transformer service to handle the load. This may require additional infrastructure and configuration.



`Pub-Sub ---> Transformer ---> DB-Writer ---> BigTable-DB`


Currently the flow is: 

1. populate pub sub service with ticker msgs
2. tickerstream service transform pub sub to orderbook
3. tickerstream service orderbook to pubsub msgs
4. tickerstream persistence service persists pub sub msgs to bigTable