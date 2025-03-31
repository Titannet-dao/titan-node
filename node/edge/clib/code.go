package clib

// clib error define
const (
	CodeUntracked                 = -999 + iota // -999
	CodeRepoPathInvalid                         // -998  repo path error, could cause from path not exists, or removed by others, or repo path permission denied
	CodeClibRequestInvalid                      // -997  clib request invalid, need to fix
	CodeClibResponseInvalid                     // -996  clib response invalid, need to fix
	CodeSignHashEmpty                           // -995  args Hash can not empty
	CodeOpenPathError                           // -994  repo path open failed, due to a invalid user permission or repo path was occupied by other process or other error
	CodePrivateKeyError                         // -993  private key error, could caused by repo path issues, or need to fix
	CodeConfigInvalid                           // -992  config file in repo is config.toml, check the specified message to fix
	CodeConfigStoragePathInvalid                // -991  storage path in config.toml is invalid, check the specified message to fix
	CodeConfigLocalDiskLoadError                // -990  load config from local disk error, need to fix
	CodeConfigFilePermissionError               // -989  config file have incorrect permission, make sure it's owned by user or belong to a group that user belong to
	CodeDownloadFileFailed                      // -988  file download failed. please check the specified message to fix
	CodeNewEdgeAPIErr                           // -987  new edge api error, please make sure daemon start
	CodeEdgeRestartFailed                       // -986  restart edge api failed, please check the specified message to fix, or restart app manually
	CodeEdgeNetworkErr                          // -985  edge network error, check if the network reachable or you can reach hub node
	CodeWebServerErr                            // -984  start web server error, check if config is right
	CodeFreeUpDiskInProgress                    // -983  Free up disk in progress, due to unavailable disk space
	CodeReqDiskFreeErr                          // -982  Request disk free error, due to scheduler error
	CodeStateFreeUpDiskErr                      // -981  State free up disk error, due to scheduler error

	CodeDaemonAlreadyStart    // -980  daemon already start
	CodeDaemonIsStarting      // -979  daemon starting
	CodeDaemonStartFailed     // -978  daemon start failed
	CodeDaemonAlreadyStoppped // -977  daemon is stopped
	CodeDaemonStopFailed      // -976  daemon stop failed
	CodeDaemonLogSetError     // - 975  daemon log set error

	CodeSuccess    = 0
	CodeDefaultErr = -1
)
