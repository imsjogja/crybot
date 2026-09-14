//! BaseSwap Router bindings.
//! Alamat: 0x327Df1E6de05895d2ab08513aaDD9313Fe505d86

use alloy_sol_types::sol;

sol! {
    /// BaseSwap Router interface.
    /// Alamat: 0x327Df1E6de05895d2ab08513aaDD9313Fe505d86
    #[derive(Debug, PartialEq)]
    interface IBaseSwapRouter {
        function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external returns (uint256[] memory amounts);
        function swapExactETHForTokens(uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external payable returns (uint256[] memory amounts);
        function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, address[] calldata path, address to, uint256 deadline) external returns (uint256[] memory amounts);
        function getAmountsOut(uint256 amountIn, address[] calldata path) external view returns (uint256[] memory amounts);

        event PairCreated(address indexed token0, address indexed token1, address pair);
    }
}

/// Alamat BaseSwap Router di Base Network.
pub const BASESWAP_ROUTER_ADDRESS: &str = "0x327Df1E6de05895d2ab08513aaDD9313Fe505d86";
