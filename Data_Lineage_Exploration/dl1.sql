-- Create a table for storing users
CREATE TABLE Users (
    UserID INT PRIMARY KEY AUTO_INCREMENT,
    Username VARCHAR(50) NOT NULL UNIQUE,
    Email VARCHAR(100) NOT NULL UNIQUE,
    DateOfBirth DATE,
    CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a table for storing roles
CREATE TABLE Roles (
    RoleID INT PRIMARY KEY AUTO_INCREMENT,
    RoleName VARCHAR(50) NOT NULL UNIQUE
);

-- Create a table for storing user roles (many-to-many relationship)
CREATE TABLE UserRoles (
    UserID INT,
    RoleID INT,
    PRIMARY KEY (UserID, RoleID),
    FOREIGN KEY (UserID) REFERENCES Users(UserID) ON DELETE CASCADE,
    FOREIGN KEY (RoleID) REFERENCES Roles(RoleID) ON DELETE CASCADE
);

-- Create a table for storing orders
CREATE TABLE Orders (
    OrderID INT PRIMARY KEY AUTO_INCREMENT,
    UserID INT,
    OrderDate DATE NOT NULL,
    TotalAmount DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (UserID) REFERENCES Users(UserID) ON DELETE SET NULL
);

-- Create a table for storing order items
CREATE TABLE OrderItems (
    OrderItemID INT PRIMARY KEY AUTO_INCREMENT,
    OrderID INT,
    ProductName VARCHAR(100) NOT NULL,
    Quantity INT NOT NULL CHECK (Quantity > 0),
    UnitPrice DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (OrderID) REFERENCES Orders(OrderID) ON DELETE CASCADE
);

-- Create a table for storing product reviews
CREATE TABLE ProductReviews (
    ReviewID INT PRIMARY KEY AUTO_INCREMENT,
    ProductName VARCHAR(100) NOT NULL,
    ReviewText TEXT,
    Rating INT CHECK (Rating BETWEEN 1 AND 5),
    ReviewDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a table for storing product categories
CREATE TABLE Categories (
    CategoryID INT PRIMARY KEY AUTO_INCREMENT,
    CategoryName VARCHAR(50) NOT NULL UNIQUE
);

-- Create a table for storing products and their categories
CREATE TABLE Products (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    ProductName VARCHAR(100) NOT NULL,
    CategoryID INT,
    Price DECIMAL(10, 2) NOT NULL,
    StockQuantity INT NOT NULL CHECK (StockQuantity >= 0),
    FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID)
);

-- Create a table to store total order amounts by user
CREATE TABLE UserTotalOrderAmounts AS
SELECT
    Users.UserID,
    Users.Username,
    COALESCE(SUM(Orders.TotalAmount), 0) AS TotalAmount
FROM
    Users
LEFT JOIN Orders ON Users.UserID = Orders.UserID
GROUP BY
    Users.UserID, Users.Username;

-- Create a table for products and their total order item quantities
CREATE TABLE ProductTotalQuantities AS
SELECT
    Products.ProductName,
    COALESCE(SUM(OrderItems.Quantity), 0) AS TotalQuantity
FROM
    Products
LEFT JOIN OrderItems ON Products.ProductName = OrderItems.ProductName
GROUP BY
    Products.ProductName;

-- Create a table for orders with user details
CREATE TABLE OrdersWithUserDetails AS
SELECT
    Orders.OrderID,
    Orders.OrderDate,
    Users.Username,
    Orders.TotalAmount
FROM
    Orders
JOIN Users ON Orders.UserID = Users.UserID;
