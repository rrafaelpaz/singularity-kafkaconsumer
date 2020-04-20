package com.singularity.kafkaconsumer.model;


import org.springframework.cloud.gcp.data.datastore.repository.DatastoreRepository;

public class UserRepository extends DatastoreRepository<User, Long> {



}
