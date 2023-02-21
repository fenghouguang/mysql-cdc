package service

var (
	_transferService *TransferService
)

func Initialize() error {
	transferService := &TransferService{
		loopStopSignal: make(chan struct{}, 1),
	}
	err := transferService.initialize()
	if err != nil {
		return err
	}
	_transferService = transferService

	return nil
}

func StartUp() {

	_transferService.StartUp()

}

func Close() {
	_transferService.Close()
}

func TransferServiceIns() *TransferService {
	return _transferService
}
