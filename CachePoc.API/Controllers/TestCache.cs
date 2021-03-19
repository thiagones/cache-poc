using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Bogus;
using System.Text.Json;
using System.Collections.Concurrent;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Diagnostics;

namespace CachePoc.API.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TestCache : ControllerBase
    {

        private readonly ILogger<TestCache> _logger;
        private readonly IDistributedCache _cache;
        private readonly SqlConnection _sqlConnection;

        public TestCache(
            ILogger<TestCache> logger,
            SqlConnection sqlConnection,
            IDistributedCache cache
            )
        {
            _logger = logger;
            _cache = cache;
            _sqlConnection = sqlConnection;
        }

        [HttpGet]
        public async Task<IActionResult> Get([FromQuery] int advisorId)
        {
            if (_sqlConnection.State != ConnectionState.Open)
            {
                await _sqlConnection.OpenAsync();
            }

            var advisorCustomers = await _sqlConnection.QueryAsync<AdvisorCustomer>
                ("SELECT AdvisorId, CustomerId FROM AdvisorCustomer WHERE AdvisorId = @pAdvisorId",
                new { pAdvisorId = advisorId });

            await _sqlConnection.CloseAsync();

            var stopwatchAdvisor = new Stopwatch();
            var stopwatchCustomer = new Stopwatch();

            Dictionary<string, string> result = new Dictionary<string, string>();

            stopwatchAdvisor.Start();
            var byAdvisor = await _cache.GetStringAsync($"AdvisorId_{advisorId}");
            var fakeCachedByAdvisorObject = JsonSerializer.Deserialize<List<FakeCachedObject>>(byAdvisor);
            stopwatchAdvisor.Stop();
            result.Add("ByAdvisor", stopwatchAdvisor.ElapsedMilliseconds.ToString());


            stopwatchCustomer.Start();
            List<FakeCachedObject> fakeCachedObjects = new List<FakeCachedObject>();
            foreach (var advisorCustomer in advisorCustomers)
            {
                var byCustomer = await _cache.GetStringAsync($"CustomerId_{advisorCustomer.CustomerId}");
                var fakeCachedByCustomerObject = JsonSerializer.Deserialize<FakeCachedObject>(byCustomer);
                fakeCachedObjects.Add(fakeCachedByCustomerObject);
            }
            stopwatchCustomer.Stop();
            result.Add("ByCustomer", stopwatchCustomer.ElapsedMilliseconds.ToString());

            return Ok();
        }

        [HttpPut("Advisor")]
        public async Task<IActionResult> SetByAdvisor()
        {
            if (_sqlConnection.State != ConnectionState.Open)
            {
                await _sqlConnection.OpenAsync();
            }

            var advisorsCustomers = await _sqlConnection.QueryAsync<AdvisorCustomer>
                ("SELECT AdvisorId, CustomerId FROM AdvisorCustomer");

            await _sqlConnection.CloseAsync();

            var advisorsIds = advisorsCustomers.Select(x => x.AdvisorId).Distinct().ToList();

            var fakeCachedObjectMock = new Faker<FakeCachedObject>();

            foreach (var advisorId in advisorsIds)
            {
                var fakes = advisorsCustomers
                    .Where(x => x.AdvisorId == advisorId)
                    .Select(x =>
                    {
                        var obj = fakeCachedObjectMock.Generate();
                        obj.AdvisorId = x.AdvisorId;
                        obj.CustomerId = x.CustomerId;
                        return obj;
                    }).ToList();

                await _cache.SetStringAsync($"AdvisorId_{advisorId}", JsonSerializer.Serialize(fakes));
            }

            return Ok();
        }

        [HttpPut("Customer")]
        public async Task<IActionResult> SetByCustomer()
        {
            if (_sqlConnection.State != ConnectionState.Open)
            {
                await _sqlConnection.OpenAsync();
            }

            var advisorsCustomers = await _sqlConnection.QueryAsync<AdvisorCustomer>
                ("SELECT AdvisorId, CustomerId FROM AdvisorCustomer WHERE AdvisorId = 564");

            await _sqlConnection.CloseAsync();

            var advisorsIds = advisorsCustomers.Select(x => x.AdvisorId).Distinct().ToList();

            var fakeCachedObjectMock = new Faker<FakeCachedObject>();

            foreach (var advisorCustomer in advisorsCustomers)
            {
                var fakeCachedObject = fakeCachedObjectMock
                    .RuleFor(x => x.Propertie_01, f => f.Random.Double(1, 1000))
                    .RuleFor(x => x.Propertie_02, f => f.Random.Double(1, 1000))
                    .RuleFor(x => x.Propertie_03, f => f.Random.Double(1, 1000))
                    .RuleFor(x => x.Propertie_04, f => f.Random.Double(1, 1000))
                    .RuleFor(x => x.Propertie_05, f => f.Random.Double(1, 1000))
                    .RuleFor(x => x.Propertie_06, f => f.Random.Double(1, 1000))
                    .RuleFor(x => x.Propertie_07, f => f.Random.String(20))
                    .RuleFor(x => x.Propertie_08, f => f.Random.String(20))
                    .RuleFor(x => x.Propertie_09, f => f.Random.String(20))
                    .RuleFor(x => x.Propertie_10, f => f.Random.String(20))
                    .RuleFor(x => x.Propertie_11, f => DateTime.Now)
                    .RuleFor(x => x.Propertie_12, f => DateTime.Now)
                    .RuleFor(x => x.Propertie_13, f => f.Random.Int(1, 2000))
                    .RuleFor(x => x.Propertie_14, f => f.Random.Int(1, 2000))
                    .RuleFor(x => x.Propertie_15, f => f.Random.Int(1, 2000))
                    .Generate();

                await _cache.SetStringAsync($"CustomerId_{advisorCustomer.CustomerId}", JsonSerializer.Serialize(fakeCachedObject));
            }

            return Ok();
        }
    }
}
