package com.feearb.mobile

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import okhttp3.OkHttpClient
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path
import java.util.concurrent.TimeUnit

interface FeeArbApi {
    @GET("api/mobile/positions")
    suspend fun getMobilePositions(): MobilePositionsResponse

    @GET("api/positions/overview")
    suspend fun getPositionsOverview(): PositionsOverviewResponse

    @GET("api/mobile/manual-defaults")
    suspend fun getManualDefaults(): ManualDefaultsResponse

    @POST("api/mobile/manual-spread")
    suspend fun getManualSpread(@Body payload: MobileManualSpreadRequest): MobileManualSpreadResponse

    @GET("api/auto-arb")
    suspend fun getAutoArb(): JsonObject

    @POST("api/auto-arb/analyze")
    suspend fun analyzeAutoArb(@Body payload: AutoArbRuleRequest): JsonObject

    @POST("api/auto-arb/rules")
    suspend fun upsertAutoArbRule(@Body payload: AutoArbRuleRequest): JsonObject

    @POST("api/position/action")
    suspend fun positionAction(@Body payload: PositionActionRequest): JsonObject

    @POST("api/manual/analyze")
    suspend fun manualAnalyze(@Body payload: ManualRequest): JsonObject

    @POST("api/manual/enter")
    suspend fun manualEnter(@Body payload: ManualRequest): JsonObject

    @POST("api/manual/exit")
    suspend fun manualExit(@Body payload: ManualRequest): JsonObject

    @POST("api/manual/roll")
    suspend fun manualRoll(@Body payload: ManualRequest): JsonObject

    @GET("api/manual/exec/{executionId}")
    suspend fun manualExecStatus(@Path("executionId") executionId: String): ManualExecStatusResponse

    @POST("api/manual/exec/{executionId}/stop")
    suspend fun stopManualExec(@Path("executionId") executionId: String): JsonObject
}

object FeeArbApiFactory {
    private val gson = GsonBuilder().create()

    fun create(baseUrl: String, remoteAccessToken: String = ""): FeeArbApi {
        val normalizedBaseUrl = if (baseUrl.endsWith("/")) baseUrl else "$baseUrl/"
        val httpClient = OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(120, TimeUnit.SECONDS)
            .writeTimeout(120, TimeUnit.SECONDS)
            .callTimeout(120, TimeUnit.SECONDS)
            .addInterceptor { chain ->
                val request = chain.request()
                val nextRequest = if (remoteAccessToken.isNotBlank()) {
                    request.newBuilder()
                        .header("X-FeeArb-Token", remoteAccessToken.trim())
                        .build()
                } else {
                    request
                }
                chain.proceed(nextRequest)
            }
            .build()
        return Retrofit.Builder()
            .baseUrl(normalizedBaseUrl)
            .client(httpClient)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .build()
            .create(FeeArbApi::class.java)
    }
}
