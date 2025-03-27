package httpserver

import "net/http"

func (hs *HttpServer) uploadv4Handler(w http.ResponseWriter, r *http.Request) {
	log.Debug("uploadv4Handler")
	// setAccessControlAllowForHeader(w)
	// if r.Method == http.MethodOptions {
	// 	return
	// }
	// if r.Method != http.MethodPost {
	// 	uploadResult(w, -1, fmt.Sprintf("only allow post method, http status code %d", http.StatusMethodNotAllowed))
	// 	return
	// }

	// userID, pass, err := verifyToken(r, hs.apiSecret)
	// if err != nil {
	// 	log.Errorf("verfiy token error: %s", err.Error())
	// 	uploadResult(w, -1, fmt.Sprintf("%s, http status code %d", err.Error(), http.StatusUnauthorized))
	// 	return
	// }

	// log.Infof("user %s upload tmpfile with range of %d - %d", userID, bytesFrom, bytesTo)

	// // limit max concurrent
	// semaphore <- struct{}{}
	// defer func() { <-semaphore }()

	// // limit size
	// r.Body = http.MaxBytesReader(w, r.Body, int64(hs.maxSizeOfUploadFile))

	// //
	// UnrouterHandler, err := handler.NewUnroutedHandler()

}
