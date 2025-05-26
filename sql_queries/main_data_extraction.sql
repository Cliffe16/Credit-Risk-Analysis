SELECT
    L.LoanID,
    L.PrincipalAmount AS LoanPrincipal,
    L.TotalRepayable,
    L.Status AS LoanStatus,
    L.DisbursementDate,
    L.DueDate,
    DATEDIFF(day, L.DisbursementDate, L.DueDate) AS LoanTermActualDays,
    L.DaysDelayed,
    LA.ApplicationDate,
    LA.AmountRequested,
    LA.TermDays AS LoanTermRequestedDays,
    LA.Purpose AS LoanPurpose,
    P.ProductName,
    P.Category AS LoanCategory,
    P.InterestRate AS ProductInterestRate,
    P.ProcessingFee AS ProductProcessingFee,
    C.CustomerID,
    DATEDIFF(YEAR, C.DateOfBirth, LA.ApplicationDate) AS AgeAtApplication,
    C.Gender,
    C.County,
    C.EmploymentStatus,
    C.MonthlyIncome,
    C.EducationLevel,
    C.MaritalStatus,
    C.HasBankAccount,
    C.MobileMoneyProvider,
    C.MonthlyMobileMoneyVolume,
    C.RegistrationDate AS CustomerRegistrationDate,
    CCI.CreditScore,
    CCI.PaymentHistoryScore,
    CCI.CreditUtilization,
    CCI.CreditHistoryLength,
    CCI.TotalLoansTaken AS PreviousLoansTaken,
    CCI.TotalAmountBorrowed AS PreviousTotalBorrowed,
    CCI.TotalAmountRepaid AS PreviousTotalRepaid,
    CCI.ActiveLoans AS PreviousActiveLoans,
    CCI.TimesDefaulted AS PreviousDefaults,
    CCI.LastDefaultDate AS PreviousLastDefaultDate,
    CCI.CRBListed,
    CCI.CRBListingDate,
    CCI.CurrentLoanTier,
    CCI.MaxEligibleLoanAmount,
    CCI.ConsecutiveOnTimeRepayments,
    CCI.OverdraftLimit AS CustomerOverdraftLimit,
    CCI.TimesOverdrafted AS CustomerTimesOverdrafted,
    (SELECT COUNT(*) FROM MobileMoneyTransactions MMT WHERE MMT.CustomerID = C.CustomerID
                                                        AND MMT.TransactionDate < LA.ApplicationDate
                                                        AND MMT.IsOverdraft = 1
                                                        AND MMT.TransactionDate >= DATEADD(month, -3, LA.ApplicationDate)) AS OverdraftLast3Months,
   (SELECT AVG(MMT.Amount) FROM MobileMoneyTransactions MMT WHERE MMT.CustomerID = C.CustomerID
                                                            AND MMT.TransactionType = 'Deposit'
                                                            AND MMT.TransactionDate < LA.ApplicationDate
                                                            AND MMT.TransactionDate >= DATEADD(month, -6, LA.ApplicationDate)) AS AvgDepositLast6Months
FROM Loans L
JOIN LoanApplications LA ON L.ApplicationID = LA.ApplicationID
JOIN Customers C ON LA.CustomerID = C.CustomerID
JOIN LoanProducts P ON LA.ProductID = P.ProductID
JOIN CustomerCreditInfo CCI ON C.CustomerID = CCI.CustomerID
WHERE L.Status IN ('Paid', 'Defaulted');