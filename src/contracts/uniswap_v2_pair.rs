//! Uniswap V2-compatible pair bindings for read-only reserve queries.

use alloy_sol_types::sol;

sol! {
    #[derive(Debug, PartialEq)]
    interface IUniswapV2Pair {
        function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    }
}
