//! Uniswap V3 SwapRouter02, NonfungiblePositionManager & QuoterV2 bindings.
//! - SwapRouter02: 0x2626664c2603336E57B271c5C0b26F421741e481
//! - NonfungiblePositionManager: 0xC36442b4a4522E871399CD717aBDD847Ab11FE88

use alloy_sol_types::sol;

sol! {
    /// Uniswap V3 SwapRouter02 interface.
    /// Alamat: 0x2626664c2603336E57B271c5C0b26F421741e481
    #[derive(Debug, PartialEq)]
    interface IUniswapV3Router {
        struct ExactInputSingleParams {
            address tokenIn;
            address tokenOut;
            uint24 fee;
            address recipient;
            uint256 amountIn;
            uint256 amountOutMinimum;
            uint160 sqrtPriceLimitX96;
        }

        function exactInputSingle(ExactInputSingleParams calldata params) external payable returns (uint256 amountOut);

        struct ExactInputParams {
            bytes path;
            address recipient;
            uint256 amountIn;
            uint256 amountOutMinimum;
        }

        function exactInput(ExactInputParams calldata params) external payable returns (uint256 amountOut);
        function multicall(bytes[] calldata data) external payable returns (bytes[] memory results);

        event PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool);
    }
}

sol! {
    /// Uniswap V3 NonfungiblePositionManager interface.
    /// Alamat: 0xC36442b4a4522E871399CD717aBDD847Ab11FE88
    #[derive(Debug, PartialEq)]
    interface IUniswapV3PositionManager {
        struct MintParams {
            address token0;
            address token1;
            uint24 fee;
            int24 tickLower;
            int24 tickUpper;
            uint256 amount0Desired;
            uint256 amount1Desired;
            uint256 amount0Min;
            uint256 amount1Min;
            address recipient;
            uint256 deadline;
        }

        function mint(MintParams calldata params) external payable returns (uint256 tokenId, uint128 liquidity, uint256 amount0, uint256 amount1);
        function increaseLiquidity(uint256 tokenId, uint256 amount0Desired, uint256 amount1Desired, uint256 amount0Min, uint256 amount1Min, uint256 deadline) external returns (uint128 liquidity, uint256 amount0, uint256 amount1);
        function decreaseLiquidity(uint256 tokenId, uint128 liquidity, uint256 amount0Min, uint256 amount1Min, uint256 deadline) external returns (uint256 amount0, uint256 amount1);
        function collect(uint256 tokenId, address recipient, uint128 amount0Max, uint128 amount1Max) external returns (uint256 amount0, uint256 amount1);
        function positions(uint256 tokenId) external view returns (uint96 nonce, address operator, address token0, address token1, uint24 fee, int24 tickLower, int24 tickUpper, uint128 liquidity, uint256 feeGrowthInside0LastX128, uint256 feeGrowthInside1LastX128, uint128 tokensOwed0, uint128 tokensOwed1);
    }
}

sol! {
    /// Uniswap V3 QuoterV2 interface.
    #[derive(Debug, PartialEq)]
    interface IUniswapV3QuoterV2 {
        function quoteExactInputSingle(address tokenIn, address tokenOut, uint24 fee, uint256 amountIn, uint160 sqrtPriceLimitX96) external returns (uint256 amountOut, uint160 sqrtPriceX96After, uint32 initializedTicksCrossed, uint256 gasEstimate);
    }
}

/// Alamat Uniswap V3 SwapRouter02 di Base Network.
pub const UNISWAP_V3_ROUTER_ADDRESS: &str = "0x2626664c2603336E57B271c5C0b26F421741e481";

/// Alamat Uniswap V3 NonfungiblePositionManager di Base Network.
pub const UNISWAP_V3_NFT_MANAGER_ADDRESS: &str = "0xC36442b4a4522E871399CD717aBDD847Ab11FE88";
