CREATE DATABASE CarRental;
GO

USE CarRental;
GO

CREATE TABLE Sessions (
    SessionID VARCHAR(50) PRIMARY KEY,
    StartTime DATETIME,
    EndTime DATETIME,
    SessionDuration FLOAT,
    CarReturnedLate BIT,
    CarWasDamaged BIT,
    Comments VARCHAR(5000)
);


CREATE TABLE FileImportLog (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    FileName VARCHAR(255),
    ImportedSuccessfully BIT,
    ImportTimestamp DATETIME DEFAULT GETDATE()
);