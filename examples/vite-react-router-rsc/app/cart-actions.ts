"use server";

import { getCurrentDdRequestContext } from "./dd-context";

export async function addToCart(formData: FormData) {
  const dd = getCurrentDdRequestContext();
  await dd.storefront.addToCart(
    String(formData.get("slug") ?? ""),
    Number(formData.get("quantity") ?? 1),
  );
}

export async function checkoutCart(formData: FormData) {
  const dd = getCurrentDdRequestContext();
  await dd.storefront.checkout(String(formData.get("email") ?? ""));
}

export async function clearCart(_formData: FormData) {
  const dd = getCurrentDdRequestContext();
  await dd.storefront.clearCart();
}
