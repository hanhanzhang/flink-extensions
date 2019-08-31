package com.sdu.flink.rpc.akka;

import akka.actor.ActorSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceConfiguration;

import java.util.concurrent.CompletableFuture;

/**
 * Rpc组件概述
 *
 * 1: RpcGateway
 *
 *  RpcGateway提供获取其代理的RpcEndpoint的地址
 *
 * 2: RpcEndpoint
 *
 *  RpcEndpoint是对RPC框架提供具体服务的实体的抽象, 所有提供远程调用方法的组件都必须继承该类.
 *
 *  同一个RpcEndpoint的所有RPC调用都运行在同一个线程中, 故无并发执行的线程安全问题.
 *
 * 3: FencedRpcEndpoint
 *
 *  FencedRpcEndpoint在调用RPC方法时携带token信息, 只有当调用方提供了token和endpoint的token一致时才允许调用.
 *
 * 4: RpcServer
 *
 *  RpcServer是RpcEndpoint自身代理的对象(self gateway). RpcServer是RpcService.startServer()返回的RpcEndpoint的代理对象.
 *
 *  每个RpcEndpoint有属性RpcServer, 通过 getSelfGateway() 获取自身的代理.
 *
 * 5: RpcService
 *
 *
 *
 * @author hanhan.zhang
 * */
public class AkkaBootstrap {

	// 通信协议
	public interface ServerClockRpcGateway extends RpcGateway {

		long serverClock();

	}


	// 通信协议具体实现
	public static class ServerClockRpcEndpoint extends RpcEndpoint implements ServerClockRpcGateway {

		ServerClockRpcEndpoint(RpcService rpcService, String endpointId) {
			super(rpcService, endpointId);
		}

		public long serverClock() {
			return System.currentTimeMillis();
		}
	}

	public static void main(String[] args) throws Exception {
		ActorSystem actorSystem = AkkaUtils.createDefaultActorSystem();

		// AkkaRpcService
		AkkaRpcService rpcService = new AkkaRpcService(actorSystem, AkkaRpcServiceConfiguration.defaultConfiguration());

		// RpcEndpoint构造函数
		// 1: AkkaRpcService.startServer()向ActorSystem注册Actor
		// 2:
		ServerClockRpcEndpoint serverClockRpcEndpoint = new ServerClockRpcEndpoint(rpcService, "serverClockRpcEndpoint");
		serverClockRpcEndpoint.start();

		// 实质RpcServer的代理类
		ServerClockRpcGateway serverClockRpcGateway = serverClockRpcEndpoint.getSelfGateway(ServerClockRpcGateway.class);
		long serverTimestamp = serverClockRpcGateway.serverClock();

		System.out.println("Server clock timestamp: " + serverTimestamp);

		// 连接远端的RpcEndpoint, 使用 RpcService.connect(address, protocol)
		// 模拟
		CompletableFuture<ServerClockRpcGateway> rpcGatewayFuture = rpcService.connect(serverClockRpcEndpoint.getAddress(), ServerClockRpcGateway.class);
		ServerClockRpcGateway remoteClockRpcGateway = rpcGatewayFuture.get();
		serverTimestamp = remoteClockRpcGateway.serverClock();
		System.out.println("Remote server clock timestamp: " + serverTimestamp);

		rpcService.stopService();
	}

}
