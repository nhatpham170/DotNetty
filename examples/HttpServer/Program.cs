// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace HttpServer
{
    using System;
    using System.IO;
    using System.Net;
    using System.Runtime;
    using System.Runtime.InteropServices;
    using System.Security.Cryptography.X509Certificates;
    using System.Threading.Tasks;
    using DotNetty.Buffers;
    using DotNetty.Codecs.Http;
    using DotNetty.Common;
    using DotNetty.Handlers.Streams;
    using DotNetty.Handlers.Tls;
    using DotNetty.Transport.Bootstrapping;
    using DotNetty.Transport.Channels;
    using DotNetty.Transport.Channels.Sockets;
    using DotNetty.Transport.Libuv;
    using Examples.Common;

    class Program
    {
        static Program()
        {
            ResourceLeakDetector.Level = ResourceLeakDetector.DetectionLevel.Disabled;
        }
        static async Task RunServerAsync2()
        {
            int port = 7686;
            var bossGroup = new MultithreadEventLoopGroup(1); // Event loop group
            var workerGroup = new MultithreadEventLoopGroup();            

            try
            {
                var bootstrap = new ServerBootstrap();
                bootstrap.Group(bossGroup, workerGroup)
                    .Channel<TcpServerChannel>()
                    .ChildHandler(new ActionChannelInitializer<IChannel>(channel =>
                    {
                        var pipeline = channel.Pipeline;

                        // Thêm HTTP codec (encoder/decoder)
                        pipeline.AddLast(new HttpServerCodec());

                        // Xử lý dữ liệu chunked
                        pipeline.AddLast(new ChunkedWriteHandler<object>());

                        // Custom handler để xử lý request và gửi chunked response
                        pipeline.AddLast(new HttpChunkedResponseHandler());
                    }));

                // Bind server đến cổng
                IChannel bootstrapChannel = await bootstrap.BindAsync(IPAddress.IPv6Any, port);
                //IChannel boundChannel = await bootstrap.BindAsync(port);
                Console.WriteLine($"HTTP Server started on port {port}");
                Console.ReadLine();
                // Giữ server chạy
                //await boundChannel.CloseCompletion;
            }
            finally
            {
                await Task.WhenAll(bossGroup.ShutdownGracefullyAsync(), workerGroup.ShutdownGracefullyAsync());
            }
        }
        static async Task RunServerAsync()
        {
            Console.WriteLine(
                $"\n{RuntimeInformation.OSArchitecture} {RuntimeInformation.OSDescription}"
                + $"\n{RuntimeInformation.ProcessArchitecture} {RuntimeInformation.FrameworkDescription}"
                + $"\nProcessor Count : {Environment.ProcessorCount}\n");

            bool useLibuv = ServerSettings.UseLibuv;
            Console.WriteLine("Transport type : " + (useLibuv ? "Libuv" : "Socket"));

            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
            }

            Console.WriteLine($"Server garbage collection: {GCSettings.IsServerGC}");
            Console.WriteLine($"Current latency mode for garbage collection: {GCSettings.LatencyMode}");

            IEventLoopGroup group;
            IEventLoopGroup workGroup;
            group = new MultithreadEventLoopGroup(1);
            workGroup = new MultithreadEventLoopGroup();
            //if (useLibuv)
            //{
            //    var dispatcher = new DispatcherEventLoopGroup();
            //    group = dispatcher;
            //    workGroup = new WorkerEventLoopGroup(dispatcher);
            //}
            //else
            //{
            //    group = new MultithreadEventLoopGroup(1);
            //    workGroup = new MultithreadEventLoopGroup();
            //}

            X509Certificate2 tlsCertificate = null;
            if (ServerSettings.IsSsl)
            {
                tlsCertificate = new X509Certificate2(Path.Combine(ExampleHelper.ProcessDirectory, "dotnetty.com.pfx"), "password");
            }
            try
            {
                var bootstrap = new ServerBootstrap();
                bootstrap.Group(group, workGroup);

                if (useLibuv)
                {
                    bootstrap.Channel<TcpServerChannel>();
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                        || RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                    {
                        bootstrap
                            .Option(ChannelOption.SoReuseport, true)
                            .ChildOption(ChannelOption.SoReuseaddr, true);
                    }
                }
                else
                {
                    bootstrap.Channel<TcpServerSocketChannel>();
                }

                bootstrap
                    .Option(ChannelOption.SoBacklog, 8192)
                    .Option(ChannelOption.SoKeepalive, true)
                    .ChildHandler(new ActionChannelInitializer<IChannel>(channel =>
                    {
                        IChannelPipeline pipeline = channel.Pipeline;
                        if (tlsCertificate != null)
                        {
                            pipeline.AddLast(TlsHandler.Server(tlsCertificate));
                        }
                        //pipeline.AddLast("encoder", new HttpResponseEncoder());
                        //pipeline.AddLast("decoder", new HttpRequestDecoder(4096, 8192, 8192, false));
                        //pipeline.AddLast("handler", new HelloServerHandler());
                        pipeline.AddLast("encoder", new HttpServerCodec());
                        //pipeline.AddLast("decoder", new HttpRequestDecoder(4096, 8192, 8192, false));                        
                        pipeline.AddLast(new HttpObjectAggregator(65536));
                        pipeline.AddLast("decoder", new ChunkedWriteHandler<object>());
                        pipeline.AddLast("handler", new HttpChunkedResponseHandler());

                    }));

                IChannel bootstrapChannel = await bootstrap.BindAsync(IPAddress.IPv6Any, ServerSettings.Port);

                Console.WriteLine($"Httpd started. Listening on {bootstrapChannel.LocalAddress}");
                Console.ReadLine();

                await bootstrapChannel.CloseAsync();
            }
            finally
            {
                group.ShutdownGracefullyAsync().Wait();
            }
        }

        static void Main() => RunServerAsync().Wait();
    }
}