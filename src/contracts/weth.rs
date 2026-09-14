//! WETH (Wrapped Ether) contract bindings untuk Base Network.
//! Alamat: 0x4200000000000000000000000000000000000006

use alloy_sol_types::sol;

sol! {
    /// WETH (Wrapped Ether) on Base Network.
    /// Alamat: 0x4200000000000000000000000000000000000006
    #[derive(Debug, PartialEq)]
    interface IWETH {
        function deposit() external payable;
        function withdraw(uint256 amount) external;
        function balanceOf(address account) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
        function transfer(address to, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);

        event Deposit(address indexed dst, uint256 wad);
        event Withdrawal(address indexed src, uint256 wad);
        event Transfer(address indexed from, address indexed to, uint256 value);
        event Approval(address indexed owner, address indexed spender, uint256 value);
    }
}

/// Alamat WETH di Base Network.
pub const WETH_ADDRESS: &str = "0x4200000000000000000000000000000000000006";
