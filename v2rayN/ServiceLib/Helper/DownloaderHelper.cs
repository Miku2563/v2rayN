using System.Diagnostics;
using Downloader;
using DownloadService = Downloader.DownloadService;

namespace ServiceLib.Helper;

/// <summary>
///     预热 + 滑动窗口测速计算器。
///     预热期内不上报;预热后每隔 reportSec 返回最近 windowSec 秒内的平均速度(MB/s)。
/// </summary>
public sealed class SpeedMeter(double warmupSec = 0.5, double windowSec = 10.0, double reportSec = 0.6)
{
    private readonly Queue<(double T, long Bytes)> _samples = new();
    private readonly Lock _lock = new();
    private bool _warmed;
    private double _lastReport;

    /// <summary>
    ///     传入"从开始计时到现在的秒数"和"累计已接收字节数"。
    ///     需要上报时返回速度(MB/s),否则返回 null。
    /// </summary>
    public double? Update(double elapsedSec, long totalBytes)
    {
        lock (_lock)
        {
            if (!_warmed)
            {
                if (elapsedSec < warmupSec)
                {
                    return null;
                }

                // 预热结束,以此刻作为窗口起点
                _warmed = true;
                _samples.Enqueue((elapsedSec, totalBytes));
                _lastReport = elapsedSec;
                return null;
            }

            _samples.Enqueue((elapsedSec, totalBytes));

            // 丢掉窗口之外的旧样本,至少保留一个
            while (_samples.Count > 1 && elapsedSec - _samples.Peek().T > windowSec)
            {
                _samples.Dequeue();
            }

            if (elapsedSec - _lastReport < reportSec)
            {
                return null;
            }

            var first = _samples.Peek();
            var dt = elapsedSec - first.T;
            if (dt <= 0)
            {
                return null;
            }

            _lastReport = elapsedSec;
            return (totalBytes - first.Bytes) / dt / 1_000_000.0;
        }
    }
}

public class DownloaderHelper : IDisposable
{
    public HttpMessageHandler Handler { get; init; } = HttpClientHelper.Instance.DirectHandler;

    public bool DisposeHandler { get; init; } = false;

    public async Task<string?> DownloadStringAsync(string url,
        CancellationToken cancellationToken = default)
    {
        if (url.IsNullOrEmpty())
        {
            return null;
        }

        var headers = new WebHeaderCollection();
        var uri = new Uri(url);
        if (uri.UserInfo.IsNotEmpty())
        {
            headers.Add(HttpRequestHeader.Authorization, "Basic " + Utils.Base64Encode(uri.UserInfo));
        }

        var requestConfiguration = new RequestConfiguration
        {
            Headers = headers,
        };
        var downloadOpt = new DownloadConfiguration
        {
            MaxTryAgainOnFailure = 2,
            RequestConfiguration = requestConfiguration,
            CustomHttpMessageHandlerFactory = GetHandler,
        };

        await using var downloader = new DownloadService(downloadOpt);
        downloader.DownloadFileCompleted += (sender, value) =>
        {
            if (value.Error != null)
            {
                throw value.Error;
            }
        };

        await using var stream = await downloader.DownloadFileTaskAsync(url, cancellationToken);
        using StreamReader reader = new(stream);

        return await reader.ReadToEndAsync(cancellationToken);
    }

    public async Task DownloadDataAsync4Speed(string url, Action<string> onProgress,
        CancellationToken cancellationToken = default)
    {
        if (url.IsNullOrEmpty())
        {
            throw new ArgumentNullException(nameof(url));
        }

        var downloadOpt = new DownloadConfiguration
        {
            MaxTryAgainOnFailure = 2,
            CustomHttpMessageHandlerFactory = GetHandler,
        };

        var meter = new SpeedMeter(warmupSec: 2.0, windowSec: 3.0, reportSec: 1.0);
        var sw = new Stopwatch();
        var hasValue = false;
        long received = 0;

        await using var downloader = new DownloadService(downloadOpt);

        // 从下载真正开始才计时,排除建链等待
        downloader.DownloadStarted += (sender, value) =>
        {
            sw.Restart();
        };

        downloader.DownloadProgressChanged += (sender, value) =>
        {
            if (!sw.IsRunning)
            {
                sw.Start();
            }

            received = value.ReceivedBytesSize;
            var mbps = meter.Update(sw.Elapsed.TotalSeconds, received);
            if (mbps is { } m)
            {
                hasValue = true;
                onProgress.Invoke(m.ToString("#0.0"));
            }
        };

        downloader.DownloadFileCompleted += (sender, value) =>
        {
            // 已经有窗口速度上报过,就以最后一次上报为准,不再覆盖
            if (hasValue)
            {
                return;
            }

            if (value.Error != null)
            {
                onProgress.Invoke(value.Error.Message);
            }
            else if (received > 0 && sw.Elapsed.TotalSeconds > 0)
            {
                // 时间太短没熬过预热,退化为整体平均
                var avg = received / sw.Elapsed.TotalSeconds / 1_000_000.0;
                onProgress.Invoke(avg.ToString("#0.0"));
            }
            else
            {
                onProgress.Invoke("0");
            }
        };

        await using var stream = await downloader.DownloadFileTaskAsync(url, cancellationToken);
    }

    public async Task DownloadFileAsync(FileDownloadRequest request, Action<FileDownloadState> onProgress,
        CancellationToken cancellationToken = default)
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

        var downloadOpt = new DownloadConfiguration
        {
            ChunkCount = 16,
            ParallelDownload = true,
            ParallelCount = 4,
            MinimumChunkSize = 2 * 1024 * 1024, // 2 MB
            MinimumSizeOfChunking = 4 * 1024 * 1024, // 4 MB

            CustomHttpMessageHandlerFactory = GetHandler,
        };

        await using var downloader = new DownloadService(downloadOpt);
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

    public async Task DownloadSmallFilesAsync(List<FileDownloadRequest> requests,
        Action<ReadOnlyMemory<FileDownloadState>> onProgress, CancellationToken cancellationToken = default)
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

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = 4,
            CancellationToken = cancellationToken,
        };

        await Parallel.ForEachAsync(Enumerable.Range(0, requests.Count), parallelOptions,
            async (index, parallelCancellationToken) =>
            {
                var request = requests[index];
                var downloadOpt = new DownloadConfiguration
                {
                    CustomHttpMessageHandlerFactory = GetHandler,
                };
                await using var downloader = new DownloadService(downloadOpt);
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

    private HttpMessageHandler GetHandler()
    {
        return Handler;
    }

    public void Dispose()
    {
        if (!DisposeHandler)
        {
            return;
        }
        if (Handler is IDisposable disposableHandler)
        {
            disposableHandler.Dispose();
        }
    }
}
