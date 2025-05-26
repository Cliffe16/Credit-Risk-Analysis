-- Create database
USE master;
GO

DROP DATABASE IF EXISTS QuickPesaDB;
GO
CREATE DATABASE QuickPesaDB;
GO

USE QuickPesaDB;
GO

-- Customers table
CREATE TABLE Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    FirstName NVARCHAR(50) NOT NULL,
    LastName NVARCHAR(50) NOT NULL,
    PhoneNumber VARCHAR(20) UNIQUE NOT NULL,
    IDNumber VARCHAR(20) UNIQUE,
    DateOfBirth DATE NOT NULL,
    Gender CHAR(1) CHECK (Gender IN ('M', 'F')),
    County NVARCHAR(50) NOT NULL,
    SubCounty NVARCHAR(50) NOT NULL,
    Town NVARCHAR(50) NOT NULL,
    EmploymentStatus NVARCHAR(50),
    MonthlyIncome DECIMAL(18,2),
    EducationLevel NVARCHAR(50),
    MaritalStatus NVARCHAR(20),
    HasBankAccount BIT DEFAULT 0,
    MobileMoneyProvider NVARCHAR(50),
    MonthlyMobileMoneyVolume DECIMAL(18,2),
    RegistrationDate DATETIME NOT NULL,
    LastActiveDate DATETIME,
    IsActive BIT DEFAULT 1
);

-- LoanProducts table
CREATE TABLE LoanProducts (
    ProductID INT PRIMARY KEY IDENTITY(1,1),
    ProductName NVARCHAR(100) NOT NULL,
    Category NVARCHAR(50) CHECK (Category IN ('Personal', 'Business', 'Emergency', 'Agricultural')),
    MinAmount DECIMAL(20,2) NOT NULL,
    MaxAmount DECIMAL(20,2) NOT NULL,
    InterestRate DECIMAL(5,2) NOT NULL,
    ProcessingFee DECIMAL(5,2) NOT NULL,
    MinTermDays INT NOT NULL,
    MaxTermDays INT NOT NULL,
    IsFirstTime BIT DEFAULT 0,
    CRBReporting BIT DEFAULT 1,
    Description NVARCHAR(255)
);

-- LoanApplications table
CREATE TABLE LoanApplications (
    ApplicationID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Customers(CustomerID),
    ProductID INT NOT NULL FOREIGN KEY REFERENCES LoanProducts(ProductID),
    ApplicationDate DATETIME NOT NULL,
    AmountRequested DECIMAL(20,2) NOT NULL,
    TermDays INT NOT NULL,
    Purpose NVARCHAR(255),
    Status NVARCHAR(20) NOT NULL CHECK (Status IN ('Pending', 'Approved', 'Rejected', 'Cancelled')),
    StatusDate DATETIME NOT NULL,
    RejectionReason NVARCHAR(255),
    DeviceUsed NVARCHAR(50),
    IPAddress VARCHAR(50)
);

-- Loans table
CREATE TABLE Loans (
    LoanID INT PRIMARY KEY IDENTITY(1,1),
    ApplicationID INT NOT NULL FOREIGN KEY REFERENCES LoanApplications(ApplicationID),
    DisbursementDate DATETIME NOT NULL,
    PrincipalAmount DECIMAL(30,2) NOT NULL,
    InterestAmount DECIMAL(30,2) NOT NULL,
    ProcessingFee DECIMAL(30,2) NOT NULL,
    TotalRepayable DECIMAL(30,2) NOT NULL,
    DueDate DATETIME NOT NULL,
    Status NVARCHAR(20) NOT NULL CHECK (Status IN ('Active', 'Paid', 'Defaulted', 'CRB')),
    LastPaymentDate DATETIME,
    DaysDelayed INT DEFAULT 0
);

-- Repayments table
CREATE TABLE Repayments (
    RepaymentID INT PRIMARY KEY IDENTITY(1,1),
    LoanID INT NOT NULL FOREIGN KEY REFERENCES Loans(LoanID),
    RepaymentDate DATETIME NOT NULL,
    Amount DECIMAL(30,2) NOT NULL,
    PaymentMethod NVARCHAR(50) NOT NULL,
    TransactionReference NVARCHAR(100),
    IsLate BIT DEFAULT 0,
    LateFee DECIMAL(30,2) DEFAULT 0
);

-- Mobile money transactions table
CREATE TABLE MobileMoneyTransactions (
    TransactionID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Customers(CustomerID),
    TransactionDate DATETIME NOT NULL,
    TransactionType NVARCHAR(50) CHECK (TransactionType IN ('Deposit', 'Withdrawal', 'Payment', 'Transfer')),
    Amount DECIMAL(20,2) NOT NULL,
    Balance DECIMAL(18,2),
    Counterparty NVARCHAR(100),
    Reference NVARCHAR(100),
	IsOverdraft BIT DEFAULT 0,
    OverdraftFee DECIMAL(18,2) DEFAULT 0
);

-- CustomerCreditInfo table
CREATE TABLE CustomerCreditInfo (
    CreditInfoID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Customers(CustomerID),
    CreditScore INT,
    PaymentHistoryScore INT,
    CreditUtilization DECIMAL(5,2),
    CreditHistoryLength INT,
    CreditMixScore INT,
    RecentInquiries INT,
    TotalLoansTaken INT DEFAULT 0,
    TotalAmountBorrowed DECIMAL(20,2) DEFAULT 0,
    TotalAmountRepaid DECIMAL(20,2) DEFAULT 0,
    ActiveLoans INT DEFAULT 0,
    ActiveLoanAmount DECIMAL(20,2) DEFAULT 0,
    TimesDefaulted INT DEFAULT 0,
    LastDefaultDate DATETIME,
    DaysSinceLastDefault INT,
    CRBListed BIT DEFAULT 0,
    CRBListingDate DATETIME,
    CRBListingType NVARCHAR(50),
    MobileMoneyRepaymentHistory INT,
    LastUpdated DATETIME NOT NULL,
	TimesOverdrafted INT DEFAULT 0,
    TotalOverdraftFees DECIMAL(18,2) DEFAULT 0,
    OverdraftLimit DECIMAL(18,2) DEFAULT 5000,
	CurrentLoanTier INT DEFAULT 0,          
	MaxEligibleLoanAmount DECIMAL(18,2) DEFAULT 500.00, 
	ConsecutiveOnTimeRePayments INT DEFAULT 0 
);

-- CustomerDeviceInfo table
CREATE TABLE CustomerDeviceInfo (
    DeviceID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Customers(CustomerID),
    DeviceModel NVARCHAR(100),
    OSVersion NVARCHAR(50),
    AppVersion NVARCHAR(50),
    FirstSeenDate DATETIME NOT NULL,
    LastSeenDate DATETIME NOT NULL
);

-- credit inquiries table
CREATE TABLE CreditInquiries (
    InquiryID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT NOT NULL FOREIGN KEY REFERENCES Customers(CustomerID),
    InquiryDate DATETIME NOT NULL,
    LenderName NVARCHAR(100),
    Purpose NVARCHAR(100),
    AmountRequested DECIMAL(20,2),
    Status NVARCHAR(50)
);

/*SELECT TABLE_NAME, COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE DATA_TYPE = 'decimal' AND NUMERIC_PRECISION < 20;*/
