package db

type MachineUsage struct {
	ID             uint `gorm:"primaryKey;autoIncrement"`
	MachineID      string
	Time, CPU, Ram int
}
