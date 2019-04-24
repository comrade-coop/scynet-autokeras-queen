#!/usr/bin/env bash

sleep 1
cd ../scynet-proto

grpcc -a 127.0.0.1:$1 -i -p Component.proto --eval 'client.AgentStart({ egg: { componentType: "pytorch_executor", uuid: "e1", eggData: fs.readSync("model.torch") } }, printReply)'
read -p "Press enter to continue"

grpcc -a 127.0.0.1:$1 -i -p Component.proto --eval 'client.AgentList({}, printReply)'
read -p "Press enter to continue"

grpcc -a 127.0.0.1:$1 -i -p Component.proto --eval 'client.AgentStatus({ uuid: "q1" }, printReply)'
read -p "Press enter to continue"

grpcc -a 127.0.0.1:$1 -i -p Component.proto --eval 'client.AgentStop({ uuid: "q1" }, printReply)'
read -p "Press enter to continue"

grpcc -a 127.0.0.1:$1 -i -p Component.proto --eval 'client.AgentStatus({ uuid: "q1" }, printReply)'

read -p "Press enter to continue"
