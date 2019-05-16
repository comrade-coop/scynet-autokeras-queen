import sys
from Scynet.Hatchery_pb2 import ComponentRegisterRequest, AgentRegisterRequest, ComponentUnregisterRequest
from Scynet.Hatchery_pb2_grpc import HatcheryStub
from Scynet.Shared_pb2 import Void, Agent
from Scynet.Component_pb2_grpc import ComponentServicer, add_ComponentServicer_to_server
from Scynet.Component_pb2 import AgentStatusResponse, ListOfAgents
import grpc

from concurrent import futures
import time
import logging
import uuid

from autokeras_queen_agent import Queen
from pytorch_executor_agent import TorchExecutor
from multiprocessing.managers import BaseManager

from registry import AgentRegistry


_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ComponentManager(BaseManager):
    pass


class Hatchery:
    def __init__(self, stub, componentId):
        self.componentId = str(componentId)
        self.stub = stub

    def RegisterAgent(self, model, inputs, performance):
        agent = Agent(
            uuid=str(uuid.uuid4()),
            eggData=model,
            inputs=inputs,
            componentType="pytorch_executor",
            componentId=self.componentId,
            performance=performance
        )

        print(f"Registering Agent(uuid={agent.uuid}, performance={agent.performance})")
        try:
            self.stub.RegisterAgent(AgentRegisterRequest(agent=agent))
            print("Registered")
        except Exception:
            print(sys.exc_info())
            print("Failed to register!")


class ComponentFacade(ComponentServicer):
    def __init__(self, registry, hatchery):
        self.registry = registry
        self.hatchery = hatchery

    def AgentStart(self, request, context):
        if request.egg.componentType == "pytorch_executor":
            agent = TorchExecutor(request.egg.uuid, request.egg)

        self.registry.start_agent(agent)
        return Void()

    def AgentStop(self, request, context):
        self.registry.stop_agent(request.uuid)
        return Void()

    def AgentStatus(self, request, context):
        return AgentStatusResponse(
            running=self.registry.is_running(request.uuid)
        )

    def AgentList(self, request, context):
        return ListOfAgents(agents=[agent.egg for agent in self.registry.get_all_agents()])


class LoggingInterceptor(grpc.ServerInterceptor):
    def __init__(self, logger):
        self._logger = logger

    def intercept_service(self, continuation, handler_call_details):
        print(f"{handler_call_details.method}")
        return continuation(handler_call_details)


# TODO: Rewrite with: https://github.com/google/pinject
# TODO: Use this: https://github.com/BVLC/caffe/blob/master/python/caffe/io.py#L36
class Main:
    def __init__(self, port=0):
        self.port = port
        self.channel = grpc.insecure_channel('localhost:9998')
        self.hatchery = HatcheryStub(self.channel)
        self.component_uuid = uuid.uuid4()

        ComponentManager.register('Hatchery', callable=lambda: Hatchery(self.hatchery, self.component_uuid))

    def register(self, port):
        # TODO: Better way to find the bound ip's
        request = ComponentRegisterRequest(
            uuid=str(self.component_uuid), address=f"127.0.0.1:{port}")
        request.runnerType[:] = ["autokeras_queen", "pytorch_executor"]

        print(self.hatchery.RegisterComponent(request))
        print("Component registered.")

    def serve(self):

        with ComponentManager() as manager:
            registry = AgentRegistry()

            logging_interceptor = LoggingInterceptor(
                logging.getLogger(__name__))
            server = grpc.server(
                futures.ThreadPoolExecutor(max_workers=10),
                interceptors=(logging_interceptor,))
            add_ComponentServicer_to_server(
                ComponentFacade(registry, self.hatchery), server)

            self.port = server.add_insecure_port(f"0.0.0.0:{self.port}")
            try:
                self.register(self.port)
            except Exception:
                pass

            server.start()
            print(f"Listening on: 127.0.0.1:{self.port}")

            queen = Queen(manager.Hatchery(), '30bdf7fc-8c8d-4de0-b0cf-ef65fffa7844', '331d591b-184d-4e7c-b075-9841181c05c1')
            queen.start()

            print("Queen started, now producing agents")

            try:
                while True:
                    time.sleep(_ONE_DAY_IN_SECONDS)
            except KeyboardInterrupt:
                self.hatchery.UnregisterComponent(
                    ComponentUnregisterRequest(uuid=str(main.component_uuid)))
                server.stop(0)


if __name__ == '__main__':
    try:
        logging.basicConfig()
        main = Main()
        main.serve()
    finally:
        # bookkeeping
        pass
