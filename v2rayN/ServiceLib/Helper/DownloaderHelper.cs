using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Security.Authentication;
using Downloader;

namespace ServiceLib.Helper;

public class DownloaderHelper
{
    private static readonly Lazy<DownloaderHelper> _instance = new(() => new());
    public static DownloaderHelper Instance => _instance.Value;

    private static readonly TimeSpan SpeedWarmup = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan SpeedReportInterval = TimeSpan.FromSeconds(1);
    private const int SpeedReadBufferSize = 64 * 1024;

    public async Task<string?> DownloadStringAsync(IWebProxy? webProxy, string url, string? userAgent,
        IReadOnlyDictionary<string, string>? requestHeaders = null, string? acceptHeader = null, CancellationToken cancellationToken = default)
    {
        if (url.IsNullOrEmpty())
        {
            return null;
        }

        Uri uri = new(url);
        //Authorization Header
        var headers = new WebHeaderCollection();
        if (uri.UserInfo.IsNotEmpty())
        {
            headers.Add(HttpRequestHeader.Authorization, "Basic " + Utils.Base64Encode(uri.UserInfo));
        }

        var requestConfiguration = new RequestConfiguration()
        {
            Headers = headers,
            Accept = acceptHeader,
            UserAgent = userAgent,
            ConnectTimeout = GetConnectTimeoutMs(webProxy != null),
            Proxy = webProxy
        };
        var downloadOpt = new DownloadConfiguration()
        {
            MaxTryAgainOnFailure = 2,
            RequestConfiguration = requestConfiguration,
            CustomHttpMessageHandlerFactory = () => HttpRequestHeadersHelper.CreateHandler(GetSocketsHttpHandler(requestConfiguration), requestHeaders),
        };

        await using var downloader = new Downloader.DownloadService(downloadOpt);
        downloader.DownloadFileCompleted += (sender, value) =>
        {
            if (value.Error != null)
            {
                throw value.Error;
            }
        };

        await using var stream = await downloader.DownloadFileTaskAsync(address: url, cancellationToken);
        using StreamReader reader = new(stream);

        return await reader.ReadToEndAsync(cancellationToken);
    }

    /// <summary>
    /// 测速下载：自行流式读取响应并统计字节数。
    /// 速度 = 预热结束后的平均值（十进制 MB/s）。
    /// 每秒通过 onProgress 上报一次平均速度，结束时再上报一次最终值；失败时上报错误信息。
    /// duration 到期视为正常结束；cancellationToken 取消则直接抛出。
    /// </summary>
    public async Task DownloadDataAsync4Speed(IWebProxy webProxy, string url, Func<string, Task> onProgress,
        TimeSpan duration, CancellationToken cancellationToken = default)
    {
        if (url.IsNullOrEmpty())
        {
            throw new ArgumentNullException(nameof(url));
        }

        var requestConfiguration = new RequestConfiguration()
        {
            ConnectTimeout = GetConnectTimeoutMs(true),
            Proxy = webProxy
        };

        using var client = new HttpClient(GetSocketsHttpHandler(requestConfiguration), disposeHandler: true)
        {
            Timeout = Timeout.InfiniteTimeSpan,
        };

        using var timeoutCts = new CancellationTokenSource(duration);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);
        var linkedToken = linkedCts.Token;

        var meter = new SpeedMeter(SpeedWarmup);
        var lastReport = TimeSpan.Zero;
        string? error = null;

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.UserAgent.TryParseAdd(Utils.GetVersion(false));

            using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, linkedToken);
            response.EnsureSuccessStatusCode();

            await using var stream = await response.Content.ReadAsStreamAsync(linkedToken);

            var buffer = ArrayPool<byte>.Shared.Rent(SpeedReadBufferSize);
            try
            {
                while (true)
                {
                    var read = await stream.ReadAsync(buffer.AsMemory(0, SpeedReadBufferSize), linkedToken);
                    if (read == 0)
                    {
                        break;
                    }

                    meter.AddBytes(read);

                    if (meter.Warmed && meter.Elapsed - lastReport >= SpeedReportInterval)
                    {
                        lastReport = meter.Elapsed;
                        var avg = meter.AverageMBps();
                        if (avg > 0)
                        {
                            await onProgress(avg.ToString("#0.0", CultureInfo.InvariantCulture));
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            // 测速时长到期，视为正常结束，使用已测得的平均速度
        }
        catch (Exception ex)
        {
            error = ex.Message;
        }

        var final = meter.AverageMBps();
        if (final > 0)
        {
            await onProgress(final.ToString("#0.0", CultureInfo.InvariantCulture));
        }
        else
        {
            await onProgress(error ?? "0");
        }
    }

    public async Task DownloadFileAsync(IWebProxy? webProxy, FileDownloadRequest request, Action<FileDownloadState> onProgress, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.FilePath.IsNullOrEmpty())
        {
            throw new ArgumentNullException(nameof(request.FilePath));
        }
        if (File.Exists(request.FilePath))
        {
            File.Delete(request.FilePath);
        }

        var state = new FileDownloadState
        {
            Request = request,
        };

        var requestConfiguration = new RequestConfiguration
        {
            ConnectTimeout = GetConnectTimeoutMs(webProxy != null),
            Proxy = webProxy,
        };
        var downloadOpt = new DownloadConfiguration()
        {
            ChunkCount = 16,
            ParallelDownload = true,
            ParallelCount = 4,

            MinimumChunkSize = 2 * 1024 * 1024, // 2 MB
            MinimumSizeOfChunking = 4 * 1024 * 1024, // 4 MB

            RequestConfiguration = requestConfiguration,
            CustomHttpMessageHandlerFactory = () => GetSocketsHttpHandler(requestConfiguration),
        };

        await using var downloader = new Downloader.DownloadService(downloadOpt);
        downloader.DownloadStarted += (sender, value) =>
        {
            state = state with
            {
                TotalBytes = value.TotalBytesToReceive,
            };
            onProgress.Invoke(state);
        };
        downloader.DownloadProgressChanged += (sender, value) =>
        {
            state = state with
            {
                DownloadedBytes = value.ReceivedBytesSize,
                TotalBytes = value.TotalBytesToReceive,
                SpeedBytesPerSecond = value.BytesPerSecondSpeed,
            };
            onProgress.Invoke(state);
        };
        downloader.DownloadFileCompleted += (sender, value) =>
        {
            state = state with
            {
                Completed = true,
                Error = value.Error,
            };
            onProgress.Invoke(state);
        };

        await downloader.DownloadFileTaskAsync(request.FileUrl, request.FilePath, cancellationToken);
    }

    public async Task DownloadSmallFilesAsync(IWebProxy? webProxy, List<FileDownloadRequest> requests, Action<ReadOnlyMemory<FileDownloadState>> onProgress, CancellationToken cancellationToken = default)
    {
        if (requests is not { Count: > 0 })
        {
            throw new ArgumentNullException(nameof(requests));
        }

        var states = new FileDownloadState[requests.Count];
        for (var i = 0; i < requests.Count; i++)
        {
            states[i] = new FileDownloadState
            {
                Request = requests[i],
            };
        }
        var readOnlyStates = new ReadOnlyMemory<FileDownloadState>(states);

        var requestConfiguration = new RequestConfiguration()
        {
            ConnectTimeout = GetConnectTimeoutMs(webProxy != null),
            Proxy = webProxy,

            KeepAlive = true,
        };
        using var socketsHttpHandler = GetSocketsHttpHandler(requestConfiguration);

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4,
            CancellationToken = cancellationToken,
        };

        await Parallel.ForEachAsync(Enumerable.Range(0, requests.Count), parallelOptions, async (index, parallelCancellationToken) =>
        {
            var request = requests[index];
            var downloadOpt = new DownloadConfiguration()
            {
                RequestConfiguration = requestConfiguration,
                // ReSharper disable once AccessToDisposedClosure
                CustomHttpMessageHandlerFactory = () => socketsHttpHandler,
            };
            await using var downloader = new Downloader.DownloadService(downloadOpt);
            downloader.DownloadStarted += (sender, value) =>
            {
                states[index] = states[index] with
                {
                    DownloadedBytes = 0,
                    TotalBytes = value.TotalBytesToReceive,
                    SpeedBytesPerSecond = 0,
                    Completed = false,
                };
                onProgress.Invoke(readOnlyStates);
            };
            downloader.DownloadProgressChanged += (sender, value) =>
            {
                states[index] = states[index] with
                {
                    DownloadedBytes = value.ReceivedBytesSize,
                    TotalBytes = value.TotalBytesToReceive,
                    SpeedBytesPerSecond = value.BytesPerSecondSpeed,
                    Completed = false,
                };
                onProgress.Invoke(readOnlyStates);
            };
            downloader.DownloadFileCompleted += (sender, value) =>
            {
                var newState = states[index] with { Completed = true };
                if (value.Error != null)
                {
                    newState = newState with { Error = value.Error };
                }
                states[index] = newState;
                onProgress.Invoke(readOnlyStates);
            };
            await downloader.DownloadFileTaskAsync(request.FileUrl, request.FilePath, parallelCancellationToken);
        });
    }

    // https://github.com/bezzad/Downloader/blob/a75a6e431acd6cbba6293f7afdcf676544a09174/src/Downloader/SocketClient.cs#L45
    // There is a risk of MITM attacks
    // https://github.com/bezzad/Downloader/blob/a75a6e431acd6cbba6293f7afdcf676544a09174/src/Downloader/Extensions/ExceptionHelper.cs#L111
    private static SocketsHttpHandler GetSocketsHttpHandler(RequestConfiguration config)
    {
        SocketsHttpHandler handler = new()
        {
            AllowAutoRedirect = config.AllowAutoRedirect,
            MaxAutomaticRedirections = config.MaximumAutomaticRedirections,
            AutomaticDecompression = config.AutomaticDecompression,
            PreAuthenticate = config.PreAuthenticate,
            UseCookies = config.CookieContainer != null,
            UseProxy = config.Proxy != null,
            MaxConnectionsPerServer = 1000,
            PooledConnectionIdleTimeout = config.KeepAliveTimeout,
            PooledConnectionLifetime = Timeout.InfiniteTimeSpan,
            EnableMultipleHttp2Connections = true,
            ConnectTimeout = TimeSpan.FromMilliseconds(config.ConnectTimeout),
        };

        // Set up the SslClientAuthenticationOptions for custom certificate validation
        if (config.ClientCertificates?.Count > 0)
        {
            handler.SslOptions.ClientCertificates = config.ClientCertificates;
        }

        handler.SslOptions.EnabledSslProtocols = SslProtocols.Tls13 | SslProtocols.Tls12;
        //handler.SslOptions.RemoteCertificateValidationCallback = ExceptionHelper.CertificateValidationCallBack;

        var certificateChainPolicy = CertPemManager.Instance.BuildCertificateChainPolicy();
        if (certificateChainPolicy != null)
        {
            handler.SslOptions.CertificateChainPolicy = certificateChainPolicy;
            handler.SslOptions.RemoteCertificateValidationCallback = null;
        }

        // Configure keep-alive
        if (config.KeepAlive)
        {
            handler.KeepAlivePingTimeout = config.KeepAliveTimeout;
            handler.KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests;
        }

        // Configure credentials
        if (config.Credentials != null)
        {
            handler.Credentials = config.Credentials;
            handler.PreAuthenticate = config.PreAuthenticate;
        }

        // Configure cookies
        if (handler.UseCookies && config.CookieContainer != null)
        {
            handler.CookieContainer = config.CookieContainer;
        }

        // Configure proxy
        if (handler.UseProxy && config.Proxy != null)
        {
            handler.Proxy = config.Proxy;
        }

        // Add expect header
        if (!string.IsNullOrWhiteSpace(config.Expect))
        {
            handler.Expect100ContinueTimeout = TimeSpan.FromSeconds(1);
        }

        return handler;
    }

    private int GetConnectTimeoutMs(bool isProxy)
    {
        return (int)(isProxy ? Global.ProxyDownloadConnect : Global.DirectDownloadConnect).TotalMilliseconds;
    }

    /// <summary>
    /// 速度计量：统计字节数，速度取预热结束后的平均值。
    /// 若预热前下载已结束，则回退为从开始计时的整体平均值。
    /// </summary>
    private sealed class SpeedMeter
    {
        private readonly Stopwatch _sw = Stopwatch.StartNew();
        private readonly TimeSpan _warmup;
        private long _total;
        private long _baseBytes;
        private TimeSpan _baseTime;

        public SpeedMeter(TimeSpan warmup)
        {
            _warmup = warmup;
        }

        public bool Warmed { get; private set; }

        public TimeSpan Elapsed => _sw.Elapsed;

        public void AddBytes(long count)
        {
            _total += count;
            if (!Warmed && _sw.Elapsed >= _warmup)
            {
                Warmed = true;
                _baseBytes = _total;
                _baseTime = _sw.Elapsed;
            }
        }

        /// <summary>十进制 MB/s，与原显示保持一致（÷ 1,000,000）</summary>
        public double AverageMBps()
        {
            var bytes = _total - _baseBytes;
            var secs = (_sw.Elapsed - _baseTime).TotalSeconds;
            return secs > 0 && bytes > 0 ? bytes / secs / 1_000_000d : 0;
        }
    }
}
