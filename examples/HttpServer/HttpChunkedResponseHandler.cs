using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DotNetty.Buffers;
using DotNetty.Codecs.Http;
using DotNetty.Common.Utilities;
using DotNetty.Handlers.Streams;
using DotNetty.Transport.Channels;


namespace HttpServer
{
    public class RealTimeChunkedInput : IChunkedInput<IHttpContent>
    {
        private readonly CancellationTokenSource _cts;
        private int _chunkIndex = 0;

        public RealTimeChunkedInput()
        {
            _cts = new CancellationTokenSource();
        }

        public bool IsEndOfInput => _chunkIndex >= 10; // Điều kiện dừng stream (ví dụ: 10 chunk)

        public long Length => 10;

        public long Progress => this._chunkIndex;

        public async Task<IHttpContent> ReadChunkAsync(IChannelHandlerContext context)
        {
            if (IsEndOfInput)
                return null;

            // Giả lập tạo dữ liệu chunk bất đồng bộ (ví dụ: đọc từ queue, sensor, event...)
            await Task.Delay(1000, _cts.Token); // Đợi 1s giữa các chunk

            var chunkData = $"Chunk {++_chunkIndex} - {DateTime.Now:HH:mm:ss}\n";
            var buffer = Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(chunkData));
            return new DefaultHttpContent(buffer);
        }

        public void Close()
        {
            _cts.Cancel(); // Hủy token nếu client ngắt kết nối            
        }

        public  IHttpContent ReadChunk(IByteBufferAllocator context)
        {
            //if (IsEndOfInput)
                
            if (_chunkIndex >= 10)
            {
                    return EmptyLastHttpContent.Default;
                }

            // Giả lập tạo dữ liệu chunk bất đồng bộ (ví dụ: đọc từ queue, sensor, event...)
            Task.Delay(1000, _cts.Token).GetAwaiter(); // Đợi 1s giữa các chunk

            var chunkData = $"Chunk {++_chunkIndex} - {DateTime.Now:HH:mm:ss}\n";
            var buffer = Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(chunkData));
            return new DefaultHttpContent(buffer);
        }       
    }
    public class HttpChunkedResponseHandler : SimpleChannelInboundHandler<IFullHttpRequest>
    {
        protected override async void ChannelRead0(IChannelHandlerContext ctx, IFullHttpRequest request)
        {
            if (request.Uri.Equals("/realtime-stream"))
            {
                // Thiết lập response headers
                var response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus.OK);
                response.Headers.Set(HttpHeaderNames.TransferEncoding, HttpHeaderValues.Chunked);
                response.Headers.Set(HttpHeaderNames.ContentType, "text/plain"); // SSE (tuỳ chọn)
                ctx.WriteAsync(response);

                // Bắt đầu streaming
                var chunkedInput = new RealTimeChunkedInput();
                ctx.WriteAndFlushAsync(chunkedInput); // Gửi từng chunk khi có dữ liệu
            }
            else
            {
                ctx.WriteAndFlushAsync(new DefaultFullHttpResponse(HttpVersion.Http11, HttpResponseStatus.NotFound));
            }
            //if (request.Uri.Equals("/stream"))
            //{
            //    // Thiết lập HTTP response headers cho chunked transfer
            //    var response = new DefaultHttpResponse(HttpVersion.Http11, HttpResponseStatus.OK);
            //    response.Headers.Set(HttpHeaderNames.TransferEncoding, HttpHeaderValues.Chunked);
            //    response.Headers.Set(HttpHeaderNames.ContentType, "video/x-flv");
            //    ctx.WriteAsync(response);

            //    // Gửi dữ liệu dạng chunked
            //    for (int i = 0; i < 10; i++)
            //    {
            //        var chunkData = $"Chunk {i + 1}\n";
            //        var chunk = new DefaultHttpContent(Unpooled.WrappedBuffer(Encoding.UTF8.GetBytes(chunkData)));
            //        await ctx.WriteAndFlushAsync(chunk);
            //        await Task.Delay(1000); // Giả lập độ trễ giữa các chunk
            //    }

            //    // Gửi chunk cuối cùng (LastHttpContent)
            //    await ctx.WriteAndFlushAsync(DotNetty.Codecs.Http.EmptyLastHttpContent.Default);
            //}
            //else
            //{
            //    // Xử lý các request khác
            //    var response = new DefaultFullHttpResponse(HttpVersion.Http11, HttpResponseStatus.NotFound);
            //    ctx.WriteAndFlushAsync(response);
            //}
        }

        public override void ExceptionCaught(IChannelHandlerContext ctx, Exception e)
        {
            Console.WriteLine($"Error: {e}");
            ctx.CloseAsync();
        }
    }
}

