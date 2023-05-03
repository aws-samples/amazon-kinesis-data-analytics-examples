################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

#  This file has been extended from the Apache Flink project skeleton.

import base64
from statefun import *

functions = StatefulFunctions()

GREET_REQUEST_TYPE = make_json_type(typename="com.amazonaws.samples.statefun.fns/GreetRequest")


@functions.bind(typename="com.amazonaws.samples.statefun.fns/person", specs=[ValueSpec(name="visits", type=IntType)])
def person(context: Context, message: Message):
    # update the visit count.
    visits = context.storage.visits or 0
    visits += 1
    context.storage.visits = visits

    # enrich the request with the number of vists.
    request = message.as_type(GREET_REQUEST_TYPE)
    request['visits'] = visits

    # next, we will forward a message to a special greeter function,
    # that will compute a super-doper-personalized greeting based on the
    # number of visits that this person has.
    context.send(
        message_builder(target_typename="com.amazonaws.samples.statefun.fns/greeter",
                        target_id=request['name'],
                        value=request,
                        value_type=GREET_REQUEST_TYPE))


@functions.bind(typename="com.amazonaws.samples.statefun.fns/greeter")
def greeter(context, message):
    request = message.as_type(GREET_REQUEST_TYPE)
    person_name = request['name']
    visits = request['visits']

    greeting = compute_fancy_greeting(person_name, visits)

    context.send_egress(kafka_egress_message(typename="com.amazonaws.samples.statefun/greetings",
                                             topic="greeter-egress",
                                             key=person_name,
                                             value=greeting))


def compute_fancy_greeting(name: str, seen: int):
    """
    Compute a personalized greeting, based on the number of times this @name had been seen before.
    """
    templates = ["", "Welcome %s", "Nice to see you again %s", "Third time is a charm %s"]
    if seen < len(templates):
        greeting = templates[seen] % name
    else:
        greeting = f"Nice to see you at the {seen}-nth time {name}!"

    return greeting


statefun_handler = RequestReplyHandler(functions)


def handler(event, context):
    # convert body from base64 encoding to binary protobuf
    body = base64.b64decode(event["body"])

    # invoke function handler
    res = statefun_handler.handle_sync(body)

    # return response in base64 (which will be converted back to binary protobuf by API Gateway)
    return {
       "headers": {"Content-Type": "application/octet-stream"},
       "statusCode": 200,
       "body": base64.b64encode(res).decode('ascii'),
       "isBase64Encoded": True
    }