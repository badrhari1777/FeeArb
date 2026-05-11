package com.feearb.mobile

import com.google.gson.GsonBuilder
import com.google.gson.JsonObject
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import retrofit2.http.Body
import retrofit2.http.GET
import retrofit2.http.POST
import retrofit2.http.Path

interface FeeArbApi {
    @GET("api/mobile/positions")
    suspend fun getMobilePositions(): MobilePositionsResponse

    @GET("api/mobile/manual-defaults")
    suspend fun getManualDefaults(): ManualDefaultsResponse

    @POST("api/auto-exit/rule")
    suspend fun updateAutoExitRule(@Body payload: AutoExitRuleRequest): JsonObject

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

    fun create(baseUrl: String): FeeArbApi {
        val normalizedBaseUrl = if (baseUrl.endsWith("/")) baseUrl else "$baseUrl/"
        return Retrofit.Builder()
            .baseUrl(normalizedBaseUrl)
            .addConverterFactory(GsonConverterFactory.create(gson))
            .build()
            .create(FeeArbApi::class.java)
    }
}
