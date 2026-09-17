//! Aerodrome V2 Router & Slipstream NonfungiblePositionManager bindings.
//! - Router: 0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43
//! - Slipstream NFT Manager: 0x827922686190790b37229fd06084350e74485b72

use alloy_sol_types::sol;

sol! {
    /// Pool concentrated-liquidity Aerodrome Slipstream untuk pembacaan spot
    /// price. Berbeda dari Uniswap V3 karena `slot0()` tidak memiliki
    /// return value `feeProtocol`.
    #[derive(Debug, PartialEq)]
    interface ISlipstreamPool {
        function token0() external view returns (address);
        function token1() external view returns (address);
        function slot0() external view returns (
            uint160 sqrtPriceX96,
            int24 tick,
            uint16 observationIndex,
            uint16 observationCardinality,
            uint16 observationCardinalityNext,
            bool unlocked
        );
    }
}

sol! {
    /// Aerodrome V2 Router interface.
    /// Alamat: 0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43
    #[derive(Debug, PartialEq)]
    interface IAerodromeRouter {
        struct Route {
            address from;
            address to;
            bool stable;
        }

        function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, Route[] calldata routes, address to, uint256 deadline) external returns (uint256[] memory amounts);
        function swapExactETHForTokens(uint256 amountOutMin, Route[] calldata routes, address to, uint256 deadline) external payable returns (uint256[] memory amounts);
        function swapExactTokensForETH(uint256 amountIn, uint256 amountOutMin, Route[] calldata routes, address to, uint256 deadline) external returns (uint256[] memory amounts);
        function getAmountsOut(uint256 amountIn, Route[] calldata routes) external view returns (uint256[] memory amounts);
        function getAmountsIn(uint256 amountOut, Route[] calldata routes) external view returns (uint256[] memory amounts);

    }
}

sol! {
    /// Aerodrome V2 PoolFactory interface.
    /// Alamat: 0x420DD381b31aEf6683db6B902084cB0FFECe40Da
    #[derive(Debug, PartialEq)]
    interface IAerodromePoolFactory {
        event PoolCreated(address indexed token0, address indexed token1, bool indexed stable, address pool, uint256 poolCount);
    }
}

sol! {
    /// Aerodrome Slipstream CLFactory interface.
    /// Alamat: 0xeC8E5342B19977B4eF8892e02D71DAc57b191583
    #[derive(Debug, PartialEq)]
    interface ISlipstreamFactory {
        event PoolCreated(address indexed token0, address indexed token1, int24 indexed tickSpacing, address pool);
    }
}

sol! {
    /// Aerodrome Slipstream NonfungiblePositionManager interface.
    /// Alamat: 0x827922686190790b37229fd06084350e74485b72
    #[derive(Debug, PartialEq)]
    interface ISlipstreamPositionManager {
        struct MintParams {
            address token0;
            address token1;
            int24 tickLower;
            int24 tickUpper;
            uint24 fee;
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

/// Alamat Aerodrome V2 Router di Base Network.
pub const AERODROME_ROUTER_ADDRESS: &str = "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43";

/// Alamat Slipstream NonfungiblePositionManager di Base Network.
pub const SLIPSTREAM_NFT_MANAGER_ADDRESS: &str = "0x827922686190790b37229fd06084350e74485b72";

/// Alamat Slipstream CLFactory di Base Network.
pub const SLIPSTREAM_FACTORY_ADDRESS: &str = "0xeC8E5342B19977B4eF8892e02D71DAc57b191583";
