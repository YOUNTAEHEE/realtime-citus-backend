// package com.yth.realtime.controller; // 적절한 패키지 경로로 수정하세요

// import java.io.IOException;
// import java.nio.charset.StandardCharsets;

// import org.springframework.http.HttpHeaders;
// import org.springframework.http.HttpStatus;
// import org.springframework.http.MediaType;
// import org.springframework.http.ResponseEntity;
// import org.springframework.web.bind.annotation.PostMapping;
// import org.springframework.web.bind.annotation.RequestBody;
// import org.springframework.web.bind.annotation.RequestMapping;
// import org.springframework.web.bind.annotation.RestController;

// import com.yth.realtime.dto.HistoricalDataRequest;
// import com.yth.realtime.dto.HistoricalDataResponse;
// import com.yth.realtime.service.OpcuaHistoricalService;

// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;

// @Slf4j
// @RestController
// @RequestMapping("/api/opcua/historical") // 기본 경로 설정
// @RequiredArgsConstructor
// public class OpcuaHistoricalController {

//     private final OpcuaHistoricalService opcuaHistoricalService;

//     /**
//      * 과거 OPC UA 데이터를 JSON 형식으로 조회합니다 (차트용).
//      *
//      * @param request startTime, endTime, deviceGroup, aggregationInterval 포함
//      *                (HistoricalDataRequest DTO 사용)
//      * @return HistoricalDataResponse 객체를 포함하는 ResponseEntity
//      */
//     @PostMapping // 기본 경로 ("/api/opcua/historical")에 대한 POST 요청 처리
//     public ResponseEntity<HistoricalDataResponse> getHistoricalData(@RequestBody HistoricalDataRequest request) {
//         log.info("Received historical data request: startTime={}, endTime={}, deviceGroup={}, aggregationInterval={}",
//                 request.getStartTime(), request.getEndTime(), request.getDeviceGroup(),
//                 request.getAggregationInterval());
//         try {
//             // 서비스 호출하여 데이터 조회
//             HistoricalDataResponse response = opcuaHistoricalService.getHistoricalData(
//                     request.getStartTime(),
//                     request.getEndTime(),
//                     request.getDeviceGroup(),
//                     request.getAggregationInterval());

//             if (response.isSuccess()) {
//                 // HistoricalDataResponse의 setTimeSeriesData 가 내부적으로 data 맵을 설정함
//                 log.info("Successfully fetched historical data for group {}.", request.getDeviceGroup());
//                 return ResponseEntity.ok(response);
//             } else {
//                 log.warn("Failed to fetch historical data: {}", response.getMessage());
//                 // 서비스에서 생성한 실패 응답 반환 (예: 시간 범위 오류)
//                 return ResponseEntity.badRequest().body(response);
//             }
//         } catch (Exception e) {
//             log.error("Error fetching historical data for group {}: {}", request.getDeviceGroup(), e.getMessage(), e);
//             // 예기치 않은 서버 오류 발생 시
//             HistoricalDataResponse errorResponse = HistoricalDataResponse.builder()
//                     .success(false)
//                     .message("서버 내부 오류가 발생했습니다: " + e.getMessage())
//                     .build();
//             // data 필드는 비어있거나 기본값으로 설정됨 (setTimeSeriesData 호출 안됨)
//             return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
//         }
//     }

//     /**
//      * 과거 OPC UA 데이터를 CSV 파일로 내보냅니다.
//      *
//      * @param request startTime, endTime, deviceGroup, aggregationInterval 포함
//      *                (HistoricalDataRequest DTO 사용)
//      * @return CSV 데이터를 포함하는 ResponseEntity (파일 다운로드 유도)
//      */
//     @PostMapping("/export") // "/api/opcua/historical/export"에 대한 POST 요청 처리
//     public ResponseEntity<byte[]> exportHistoricalDataToCsv(@RequestBody HistoricalDataRequest request) {
//         log.info(
//                 "Received historical data export request: startTime={}, endTime={}, deviceGroup={}, aggregationInterval={}",
//                 request.getStartTime(), request.getEndTime(), request.getDeviceGroup(),
//                 request.getAggregationInterval());
//         try {
//             // 서비스 호출하여 CSV 문자열 생성
//             String csvData = opcuaHistoricalService.exportHistoricalDataToCsv(
//                     request.getStartTime(),
//                     request.getEndTime(),
//                     request.getDeviceGroup(),
//                     request.getAggregationInterval());

//             // 파일 이름 생성 (예: opcua_export_pcs1_20231027T100000Z_20231027T110000Z.csv)
//             // 파일명에 부적합한 문자 제거 또는 변경
//             String safeStartTime = request.getStartTime().replaceAll("[:\\-]", "").replace("T", "_").replace("Z", "");
//             String safeEndTime = request.getEndTime().replaceAll("[:\\-]", "").replace("T", "_").replace("Z", "");
//             String fileName = String.format("opcua_export_%s_%s_%s_%s.csv",
//                     request.getDeviceGroup(),
//                     request.getAggregationInterval() != null ? request.getAggregationInterval() : "raw",
//                     safeStartTime, safeEndTime);

//             // HTTP 헤더 설정
//             HttpHeaders headers = new HttpHeaders();
//             headers.setContentType(MediaType.parseMediaType("text/csv; charset=UTF-8")); // UTF-8 명시
//             headers.setContentDispositionFormData("attachment", fileName); // 다운로드될 파일 이름 설정

//             // CSV 문자열을 byte 배열로 변환 (UTF-8 BOM 포함하여 Excel 호환성 높임)
//             byte[] csvBytes = ("\uFEFF" + csvData).getBytes(StandardCharsets.UTF_8); // UTF-8 BOM 추가

//             log.info("Successfully generated CSV export for group {}. Filename: {}", request.getDeviceGroup(),
//                     fileName);
//             return new ResponseEntity<>(csvBytes, headers, HttpStatus.OK);

//         } catch (IllegalArgumentException e) {
//             log.warn("Invalid request for CSV export (group {}): {}", request.getDeviceGroup(), e.getMessage());
//             // 요청 파라미터 오류 등 (400 Bad Request)
//             return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage().getBytes(StandardCharsets.UTF_8));
//         } catch (IOException e) {
//             log.error("Error generating CSV data for group {}: {}", request.getDeviceGroup(), e.getMessage(), e);
//             // CSV 생성 중 I/O 오류 발생 시 (500 Internal Server Error)
//             return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                     .body(("CSV 생성 중 오류 발생: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
//         } catch (Exception e) {
//             log.error("Error exporting historical data to CSV for group {}: {}", request.getDeviceGroup(),
//                     e.getMessage(), e);
//             // 기타 예기치 않은 오류 발생 시 (500 Internal Server Error)
//             return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                     .body(("CSV 내보내기 중 서버 오류 발생: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
//         }
//     }
// }

// package com.yth.realtime.controller; // 적절한 패키지 경로로 수정하세요

// import java.io.IOException;
// import java.io.PrintWriter;
// import java.util.ArrayList;
// import java.util.LinkedHashSet;
// import java.util.List;
// import java.util.Map;
// import java.util.Set;

// import org.springframework.web.bind.annotation.GetMapping;
// import org.springframework.web.bind.annotation.PostMapping;
// import org.springframework.web.bind.annotation.RequestBody;
// import org.springframework.web.bind.annotation.RequestMapping;
// import org.springframework.web.bind.annotation.RestController;

// import com.yth.realtime.dto.HistoricalDataRequest;
// import com.yth.realtime.dto.HistoricalDataResponse;
// import com.yth.realtime.service.OpcuaHistoricalService;

// import jakarta.servlet.http.HttpServletResponse;
// import lombok.RequiredArgsConstructor;
// import lombok.extern.slf4j.Slf4j;

// @Slf4j
// @RestController
// @RequestMapping("/api/opcua/historical") // 기본 경로 설정
// @RequiredArgsConstructor
// public class OpcuaHistoricalController {
//     private final OpcuaHistoricalService opcuaHistoricalService;

//     @PostMapping("/export")
//     public void exportHistoricalDataToCsv(
//             @RequestBody HistoricalDataRequest request,
//             HttpServletResponse response) {
//         log.info("CSV 내보내기 요청: startTime={}, endTime={}, deviceGroup={}", 
//                 request.getStartTime(), request.getEndTime(), request.getDeviceGroup());
        
//         try {
//             HistoricalDataResponse result = opcuaHistoricalService.getHistoricalData(
//                     request.getStartTime(),
//                     request.getEndTime(),
//                     request.getDeviceGroup());
    
//             if (!result.isSuccess() || result.getTimeSeriesData() == null) {
//                 response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//                 response.getWriter().write("데이터 조회 실패: " + (result.getMessage() != null ? result.getMessage() : "알 수 없는 오류"));
//                 return;
//             }
    
//             // 파일명 생성 (날짜 포함)
//             String safeStartTime = request.getStartTime().replaceAll("[:\\-]", "").replace("T", "_").replace("Z", "");
//             String fileName = String.format("opcua_export_%s_%s.csv", request.getDeviceGroup(), safeStartTime);
            
//             response.setContentType("text/csv; charset=UTF-8");
//             response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
            
//             // UTF-8 BOM 추가하여 Excel 호환성 향상
//             response.getOutputStream().write(0xEF);
//             response.getOutputStream().write(0xBB);
//             response.getOutputStream().write(0xBF);
    
//             try (PrintWriter writer = new PrintWriter(response.getOutputStream())) {
//                 List<Map<String, Object>> data = result.getTimeSeriesData();
    
//                 if (data.isEmpty()) {
//                     writer.println("데이터가 없습니다");
//                     return;
//                 }
    
//                 // CSV 헤더 추출
//                 Set<String> headers = new LinkedHashSet<>(data.get(0).keySet());
//                 writer.println(String.join(",", headers));
    
//                 // CSV 내용 출력
//                 for (Map<String, Object> row : data) {
//                     List<String> values = new ArrayList<>();
//                     for (String header : headers) {
//                         Object value = row.get(header);
//                         values.add(value != null ? value.toString() : "");
//                     }
//                     writer.println(String.join(",", values));
//                 }
//             }
//         } catch (Exception e) {
//             log.error("CSV 내보내기 오류: ", e);
//             try {
//                 response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//                 response.getWriter().write("CSV 생성 중 오류 발생: " + e.getMessage());
//             } catch (IOException ex) {
//                 log.error("응답 에러 메시지 작성 중 오류: ", ex);
//             }
//         }
//     }
// }

package com.yth.realtime.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.yth.realtime.dto.HistoricalDataRequest;
import com.yth.realtime.dto.HistoricalDataResponse;
import com.yth.realtime.service.OpcuaHistoricalService;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/api/opcua/historical")
@RequiredArgsConstructor
public class OpcuaHistoricalController {
    private final OpcuaHistoricalService opcuaHistoricalService;

    // @PostMapping
    // public HistoricalDataResponse getHistoricalData(@RequestBody HistoricalDataRequest request) {
    //     log.info("과거 데이터 요청: startTime={}, endTime={}, deviceGroup={}", 
    //             request.getStartTime(), request.getEndTime(), request.getDeviceGroup());
    //     return opcuaHistoricalService.getHistoricalData(
    //             request.getStartTime(),
    //             request.getEndTime(),
    //             request.getDeviceGroup());
    // }

    // @PostMapping("/export")
    // public void exportHistoricalDataToCsv(
    //         @RequestBody HistoricalDataRequest request,
    //         HttpServletResponse response) {
    //     log.info("CSV 내보내기 요청: startTime={}, endTime={}, deviceGroup={}", 
    //             request.getStartTime(), request.getEndTime(), request.getDeviceGroup());
        
    //     try {
    //         HistoricalDataResponse result = opcuaHistoricalService.getHistoricalData(
    //                 request.getStartTime(),
    //                 request.getEndTime(),
    //                 request.getDeviceGroup());
    
    //         if (!result.isSuccess() || result.getTimeSeriesData() == null) {
    //             response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    //             response.getWriter().write("데이터 조회 실패: " + (result.getMessage() != null ? result.getMessage() : "알 수 없는 오류"));
    //             return;
    //         }
    
    //         // 파일명 생성
    //         String safeStartTime = request.getStartTime().replaceAll("[:\\-]", "").replace("T", "_").replace("Z", "");
    //         String fileName = String.format("opcua_export_%s_%s.csv", request.getDeviceGroup(), safeStartTime);
            
    //         response.setContentType("text/csv; charset=UTF-8");
    //         response.setHeader("Content-Disposition", "attachment; filename=" + fileName);
            
    //         // UTF-8 BOM 추가
    //         response.getOutputStream().write(0xEF);
    //         response.getOutputStream().write(0xBB);
    //         response.getOutputStream().write(0xBF);
    
    //         try (PrintWriter writer = new PrintWriter(response.getOutputStream())) {
    //             List<Map<String, Object>> data = result.getTimeSeriesData();
    
    //             if (data.isEmpty()) {
    //                 writer.println("데이터가 없습니다");
    //                 return;
    //             }
    
    //             // CSV 헤더 추출
    //             Set<String> headers = new LinkedHashSet<>(data.get(0).keySet());
    //             writer.println(String.join(",", headers));
    
    //             // CSV 내용 출력
    //             for (Map<String, Object> row : data) {
    //                 List<String> values = new ArrayList<>();
    //                 for (String header : headers) {
    //                     Object value = row.get(header);
    //                     values.add(value != null ? value.toString() : "");
    //                 }
    //                 writer.println(String.join(",", values));
    //             }
    //         }
    //     } catch (Exception e) {
    //         log.error("CSV 내보내기 오류: ", e);
    //         try {
    //             response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    //             response.getWriter().write("CSV 생성 중 오류 발생: " + e.getMessage());
    //         } catch (IOException ex) {
    //             log.error("응답 에러 메시지 작성 중 오류: ", ex);
    //         }
    //     }
    // }
}