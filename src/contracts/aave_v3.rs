//! Aave V3 Pool interface bindings.
//! Alamat: 0xA238dD80c259a72e81D7e4664a9801593F98d1C5

use alloy_sol_types::sol;

sol! {
    /// Aave V3 Pool interface.
    /// Alamat: 0xA238dD80c259a72e81D7e4664a9801593F98d1C5
    #[derive(Debug, PartialEq)]
    interface IAaveV3Pool {
        function flashLoan(address receiverAddress, address[] calldata assets, uint256[] calldata amounts, uint256[] calldata interestRateModes, address onBehalfOf, bytes calldata params, uint16 referralCode) external;
        function getUserAccountData(address user) external view returns (uint256 totalCollateralBase, uint256 totalDebtBase, uint256 availableBorrowsBase, uint256 currentLiquidationThreshold, uint256 ltv, uint256 healthFactor);
        function supply(address asset, uint256 amount, address onBehalfOf, uint16 referralCode) external;
        function borrow(address asset, uint256 amount, uint256 interestRateMode, uint16 referralCode, address onBehalfOf) external;
        function repay(address asset, uint256 amount, uint256 interestRateMode, address onBehalfOf) external returns (uint256);
    }
}

/// Alamat Aave V3 Pool di Base Network.
pub const AAVE_V3_POOL_ADDRESS: &str = "0xA238dD80c259a72e81D7e4664a9801593F98d1C5";
