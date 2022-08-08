// import 'dart:async';
// // import 'dart:html';
// import 'dart:math';
// import 'package:async/async.dart';
// // import 'package:dio/dio.dart';
// // import 'package:path/path.dart' as p;
// // import 'package:universal_io/io.dart';

// class LocalUploader {
//   final Dio _dio;

//   LocalUploader(this._dio);

//   Future<Response?> upload({
//     required Stream<List<int>> file,
//     required String fileName,
//     required int fileSize,
//     required String uri,
//     Map<String, dynamic>? data,
//     CancelToken? cancelToken,
//     int? maxChunkSize,
//     Function(double)? onUploadProgress,
//     String method = 'POST',
//     String fileKey = 'media',
//   }) =>
//       UploadRequest(_dio,
//               file: file,
//               fileName: fileName,
//               fileSize: fileSize,
//               uri: uri,
//               fileKey: fileKey,
//               method: method,
//               data: data,
//               cancelToken: cancelToken,
//               maxChunkSize: maxChunkSize,
//               onUploadProgress: onUploadProgress)
//           .upload();
// }

// class UploadRequest {
//   final Dio dio;
//   final String fileName, fileKey, uri;
//   final String? method;
//   final Map<String, dynamic>? data;
//   final CancelToken? cancelToken;
//   final Stream<List<int>> file;
//   final Function(double)? onUploadProgress;
//   final int fileSize;
//   late int _maxChunkSize;
//   late ChunkedStreamReader<int> chunkedStreamReader;

//   UploadRequest(this.dio,
//       {
//       // required String this.filePath,
//       // required this.path,
//       required this.uri,
//       required this.fileKey,
//       this.method,
//       this.data,
//       this.cancelToken,
//       this.onUploadProgress,
//       required this.file,
//       required this.fileName,
//       required this.fileSize,
//       int? maxChunkSize})
//       : chunkedStreamReader = ChunkedStreamReader(file) {
//     _maxChunkSize = min(fileSize, maxChunkSize ?? fileSize);
//   }

//   Future<Response?> upload() async {
//     Response? finalResponse;
//     for (int i = 0; i < _chunksCount; i++) {
//       // final start = _getChunkStart(i);
//       // final end = _getChunkEnd(i);
//       final chunkSize = _getChunkToRead(i);
//       // final chunkStream = _getChunkStream(start, end);
//       final chunkStream = _getChunkStream(chunkSize);
//       // final formData = FormData.fromMap({
//       //   fileKey: MultipartFile(chunkStream, end - start, filename: fileName),
//       //   if (data != null) ...data!
//       // });
//       final formData = FormData.fromMap({
//         fileKey: MultipartFile(chunkStream, chunkSize, filename: fileName),
//         if (data != null) ...data!
//       });
//       finalResponse = await dio.request(
//         uri,
//         data: formData,
//         cancelToken: cancelToken,
//         options: Options(
//           method: method,
//           // headers: _getHeaders(start, end),
//           headers: _getHeaders(chunkSize),
//         ),
//         onSendProgress: (current, total) => _updateProgress(i, current, total),
//       );
//     }
//     return finalResponse;
//   }

//   // Stream<List<int>> _getChunkStream(int start, int end) =>
//   //     file.openRead(start, end);

//   Stream<List<int>> _getChunkStream(int size) =>
//       this.chunkedStreamReader.readStream(size);

//   // Updating total upload progress
//   _updateProgress(int chunkIndex, int chunkCurrent, int chunkTotal) {
//     int totalUploadedSize = (chunkIndex * _maxChunkSize) + chunkCurrent;
//     double totalUploadProgress = totalUploadedSize / fileSize;
//     onUploadProgress?.call(totalUploadProgress);
//   }

//   // Returning start byte offset of current chunk
//   int _getChunkStart(int chunkIndex) => chunkIndex * _maxChunkSize;

//   // Returning end byte offset of current chunk
//   int _getChunkEnd(int chunkIndex) =>
//       min((chunkIndex + 1) * _maxChunkSize, fileSize);

//   int _getChunkToRead(int chunkIndex) {
//     return min(_maxChunkSize, fileSize - (chunkIndex * _maxChunkSize));
//   }

//   // Returning a header map object containing Content-Range
//   // https://tools.ietf.org/html/rfc7233#section-2
//   // Map<String, dynamic> _getHeaders(int start, int end) =>
//   //     {'Content-Range': 'bytes $start-${end - 1}/$fileSize'};

//   Map<String, dynamic> _getHeaders(int size) =>
//       {'Content-Range': 'bytes $size/$fileSize'};

//   // Returning chunks count based on file size and maximum chunk size
//   int get _chunksCount => (fileSize / _maxChunkSize).ceil();
// }
