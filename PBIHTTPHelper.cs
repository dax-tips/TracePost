using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Identity.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace TracePost
{
    public class PBIRequestResponse
    {
        public JToken Data { get; set; }

        public int? HttpError { get; set; }
    }

    public class PBIHTTPHelper : IDisposable
    {
        private readonly HttpClient httpClient;
        private readonly string authorityUrl = "https://login.microsoftonline.com";
        private readonly string resourceUrl = "https://analysis.windows.net/powerbi/api";
        private readonly string apiUrl = "https://api.powerbi.com/v1.0/myorg";
        private readonly string appId = "ea0616ba-638b-4df5-95b9-636659ae5121";

        private IPublicClientApplication clientApplication;

        private readonly string tokenCacheLocalPath;
        private AuthenticationResult token;
        private readonly static SemaphoreSlim readLock = new SemaphoreSlim(1, 1);

        private readonly bool interactiveLogin = false;

        public PBIHTTPHelper(bool interactiveLogin = false)
        {

            this.httpClient = new HttpClient();
            this.httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            this.tokenCacheLocalPath = @$"{AppDomain.CurrentDomain.BaseDirectory}\pbitokencache.bin";


            clientApplication = PublicClientApplicationBuilder
                  .Create(appId)
                  .WithAuthority($"{authorityUrl}/common")                 
                  .WithDefaultRedirectUri()
                  .Build();

            this.interactiveLogin = interactiveLogin;

            if (!interactiveLogin)
            {
                clientApplication.UserTokenCache.SetBeforeAccess(args =>
                {
                    byte[] bytes = null;

                    if (File.Exists(tokenCacheLocalPath))
                    {
                        bytes = File.ReadAllBytes(tokenCacheLocalPath);
                    }

                    args.TokenCache.DeserializeMsalV3(bytes);
                });

                clientApplication.UserTokenCache.SetAfterAccess(args =>
                {
                    // if the access operation resulted in a cache update
                    if (args.HasStateChanged)
                    {
                        var bytes = args.TokenCache.SerializeMsalV3();

                        File.WriteAllBytes(tokenCacheLocalPath, bytes);
                    }
                });
            }
        }

        public async Task<AuthenticationResult> GetToken()
        {
            await readLock.WaitAsync();

            try
            {
                if (token == null || DateTime.UtcNow.AddMinutes(10) >= token.ExpiresOn)
                {
                    await RenewToken();
                }

                return this.token;
            }
            finally
            {
                readLock.Release();
            }
        }

        private async Task RenewToken()
        {
            Console.WriteLine("Getting new OAuth token");

            // On Azure Function consumption plan the tokencache is storage on the azure function blob storage

            var scopesDefault = new string[] {
                $"{resourceUrl}/.default"
            };

            AuthenticationResult result = null;

            var accounts = await clientApplication.GetAccountsAsync();

            if (accounts.Count() != 0)
            {
                // All AcquireToken* methods store the tokens in the cache, so check the cache first
                try
                {
                    result = await clientApplication.AcquireTokenSilent(scopesDefault, accounts.FirstOrDefault()).ExecuteAsync();
                }
                // A MsalUiRequiredException happened on AcquireTokenSilent. This indicates you need to call AcquireTokenInteractive to acquire a token                
                catch (MsalUiRequiredException) { }
            }

            if (result == null || string.IsNullOrEmpty(result.AccessToken))
            {
                if (this.interactiveLogin)
                {
                    result = await clientApplication
                    .AcquireTokenInteractive(scopesDefault)
                    .ExecuteAsync();
                }
                else
                {
                    result = await clientApplication.AcquireTokenWithDeviceCode(scopesDefault,
                        deviceCodeCallback =>
                        {
                            var msg = $"Go to {deviceCodeCallback.VerificationUrl} and enter device code {deviceCodeCallback.UserCode}";

                            Console.WriteLine(msg);

                            return Task.FromResult(0);

                        }).ExecuteAsync();
                }
            }

            token = result;

        }

        public async Task<PBIRequestResponse> ExecutePBIRequest(string resource
            , HttpMethod method
            , CancellationToken cancellationToken
            , string groupId = null
            , bool admin = false
            , string odataParams = null
            , bool ignoreGroup = false
            , string body = null
            , string contentType = "application/json"
    )
        {

            if (cancellationToken == CancellationToken.None)
            {
                cancellationToken = new CancellationToken();
            }

            Console.Write($"Executing PBI Request '{method.ToString()}' - '{resource}'{(odataParams != null ? $" - '{odataParams}'" : string.Empty)}");

            cancellationToken.ThrowIfCancellationRequested();

            if (!ignoreGroup && !string.IsNullOrEmpty(groupId))
            {
                resource = $"groups/{groupId}/{resource}";
            }

            var url = apiUrl;

            if (admin)
            {
                url = $"{url}/admin";
            }

            url = $"{url}/{resource}";

            if (!string.IsNullOrEmpty(odataParams))
            {
                url = $"{url}?{odataParams}";
            }

            var pbiResponse = await HttpCall(method, groupId, url, cancellationToken, body: body, contentType: contentType);

            return pbiResponse;
        }

        private const int maxNumCalls = 3;

        private async Task<PBIRequestResponse> HttpCall(HttpMethod method, string groupId, string url, CancellationToken cancellationToken
            , int numCalls = maxNumCalls, string body = null, string contentType = "application/json")
        {
            var result = new PBIRequestResponse();

            if (numCalls <= 0)
            {
                Console.WriteLine($"Reached max number of calls ({maxNumCalls}) on url '{url}'");

                result.HttpError = 429;

                return result;
            }

            //Ensure Token
            var token = await GetToken();

            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token.AccessToken);

            using (var request = new HttpRequestMessage(method, url))
            {

                if (body != null)
                {
                    request.Content = new StringContent(body);
                    request.Content.Headers.Clear();
                    request.Content.Headers.Add("Content-Type", contentType);
                }

                using (var response = await httpClient.SendAsync(request, cancellationToken))
                {
                    if (response.IsSuccessStatusCode)
                    {
                        var jsonText = await response.Content.ReadAsStringAsync();

                        var jToken = JsonConvert.DeserializeObject<JToken>(jsonText);

                        if (jToken is JObject)
                        {
                            var jObject = (JObject)jToken;

                            JToken value = null;

                            jObject.TryGetValue("value", out value);

                            if (value == null)
                            {
                                value = jObject;
                            }
                            // If group id was specified, ensure all the items have it
                            else if (!string.IsNullOrEmpty(groupId))
                            {
                                foreach (var obj in value.AsJEnumerable())
                                {
                                    obj["groupId"] = groupId;
                                }
                            }

                            result.Data = value;
                        }
                        else
                        {
                            result.Data = jToken;
                        }

                        return result;
                    }
                    else if ((int)response.StatusCode == 429)
                    {
                        //TODO: Better handle this, maybe its better to block all calls and only do 1 sleep

                        var waitSeconds = 120;

                        if (response.Headers.RetryAfter.Delta != null)
                        {
                            waitSeconds = (int)response.Headers.RetryAfter.Delta.Value.TotalSeconds + 1;
                        }

                        Console.WriteLine($"429 Error - Waiting {waitSeconds}s to repeat the call, max Tentatives: {numCalls}, url: {url}");

                        // Check if cancelled to avoid unecessary wait

                        cancellationToken.ThrowIfCancellationRequested();

                        Thread.Sleep(TimeSpan.FromSeconds(waitSeconds));

                        cancellationToken.ThrowIfCancellationRequested();

                        return await HttpCall(method, groupId, url, cancellationToken, --numCalls, body: body);
                    }
                    else
                    {
                        var httpErrorContent = await response.Content.ReadAsStringAsync();

                        // 401 and 404 errors are normal, avoid exception noise in AppInsights log as error

                        var errorMsg = $"HTTP Error: {(int)response.StatusCode} - {url} - {httpErrorContent}";

                        Console.WriteLine(errorMsg);

                        result.HttpError = (int)response.StatusCode;

                        return result;
                    }
                }
            }
        }

        public void Dispose()
        {
            if (this.httpClient != null)
            {
                this.httpClient.Dispose();
            }
        }
    }
}
